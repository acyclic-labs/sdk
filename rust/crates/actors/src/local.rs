//! Single-node Actors runtime primitives over the canonical Stream transaction.
mod applied;
mod state_binding;
pub use state_binding::{ActorStateBinding, ActorStateTransaction};
mod disposition;
mod invoke;
pub use disposition::{FailedDelivery, FailureDisposition, FailureEntry, FailureResolution};
pub use invoke::{CommittedInvocation, InvocationReceipt, InvocationTicket};
mod overlay;
/// Shared readiness registration and hibernation handoff.
pub mod ready;
pub use overlay::{Branch, Creation};
mod record;
pub use applied::{Applied, AppliedCut};
pub use record::{
    METADATA_OVERHEAD, STATE_RECORD_OVERHEAD, SUBSCRIPTION_OVERHEAD, alarm_admission,
};
mod runtime;
mod scheduling;
pub use crate::guest::Subscription;
pub use runtime::{
    Clock, ClockControl, ClockError, ControlledClock, Runtime, RuntimeLimits, SystemClock,
};
pub use scheduling::DEFAULT_SCHEDULING_SHARDS;
mod sockets;
pub use sockets::Sockets;
#[cfg(feature = "test-support")]
pub use sockets::codec_fuzz as socket_ledger_codec;

use crate::addressing::ActorReference;
use crate::guest::{
    AlarmChange, Checkpoint, Composer, Composition, GuestError, Transition, Trigger,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitId, CommitMutation, CommitOutcome, CommitReceipt,
    CommitRequest, ForkCut, IdempotencyKey, PathHead, Provenance, Record, StreamError, StreamPath,
    StreamProvider,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Typed failure at the shared actor runtime and publication boundary.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A shared call journal is malformed or cannot be addressed.
    #[error(transparent)]
    Call(#[from] crate::call::Error),
    /// Cancellation or executor loss settled the caller's original journal.
    #[error("actor invocation terminated: {receipt:?}")]
    InvocationTerminated {
        /// Original call decision; no state publication is inferred from it.
        receipt: Box<crate::call::Receipt>,
    },
    /// The original call guard refused this inline step. No effects committed;
    /// the runtime consumes only its input under the same settled marker.
    #[error("actor call was already settled")]
    CallSettled {
        /// Original call whose settled marker refused the publication.
        call: crate::call::CallPath,
        /// Exact input from the refused publication, retained through retries.
        trigger: Trigger,
    },
    /// The original scheduling clock could not produce a deadline event.
    #[error(transparent)]
    Clock(#[from] ClockError),
    /// The requested completion cut has no original host, or its host stopped.
    #[error("actor runtime completion producer stopped")]
    CompletionStopped,
    /// A delivery failed before the host could establish its completion cut.
    #[error("actor runtime completion failed: {0}")]
    CompletionFailed(Arc<Error>),
    /// The original per-agent spend balance refused the candidate.
    #[error(transparent)]
    Escrow(#[from] crate::escrow::Error),
    /// Canonical provider failure, including uncertain acknowledgement.
    #[error(transparent)]
    Stream(#[from] StreamError),
    /// The provider did not establish an outcome. Retry this exact publication;
    /// its request and participant custody remain together.
    #[error("actor publication acknowledgement is unknown: {source}")]
    Unknown {
        /// The sealed publication to pass back to [`Store::submit`].
        publication: Box<PreparedPublish>,
        /// The original uncertain provider response or transport failure.
        source: StreamError,
    },
    /// Publication has a terminal outcome, but participant settlement failed.
    #[error("actor participant settlement failed: {source}")]
    Settlement {
        /// The already decided provider outcome, never replaced by this error.
        outcome: Result<CommitOutcome, StreamError>,
        /// Why local participant settlement failed.
        source: GuestError,
    },
    /// Another owner or transition changed the exact durable cut.
    #[error("actor epoch or checkpoint changed")]
    Conflict,
    /// A composed external guard changed. Recompute the original delivery;
    /// its handler must observe the changed guard or settle its refusal.
    /// Carries the refusal's conflicts, the external change a recomputation
    /// must observe; empty when the participant refused while sealing and
    /// supplied no comparable source observation. Two empty lists do not
    /// establish that a source is unchanged.
    #[error("composed transaction precondition changed")]
    PreconditionChanged(Vec<acyclic_stream::CommitConflict>),
    /// A recomputed delivery met the identical guard refusal again: nothing
    /// external changed, so its handler cannot observe a change and another
    /// attempt would touch storage forever without progress.
    #[error("composed transaction precondition refused again with no external change")]
    PreconditionUnchanged,
    /// The actor is paused; queued work remains unconsumed.
    #[error("actor is paused")]
    Paused,
    /// The handler failed; nothing was published and the input stays queued.
    #[error(transparent)]
    Handler(#[from] GuestError),
    /// A deterministic guest failure was committed with its consumed input.
    /// Reconciliation returns this same terminal result, never a new attempt.
    #[error("actor invocation failed: {source}")]
    InvocationFailed {
        /// Original typed guest failure, including its exact detail.
        source: GuestError,
        /// Original co-committed state and failure reply.
        receipt: Box<InvocationReceipt>,
    },
    /// The authority refused an alarm consumption before its deadline: its
    /// own commit time was `authority_unix_ms`. Nothing was published.
    #[error("alarm not yet due at authority time {authority_unix_ms}")]
    AlarmNotDue {
        /// The authority's commit time at the refusal.
        authority_unix_ms: u64,
    },
    /// The actor is deleted. Its terminal state admits no claim, publication,
    /// control write or second deletion.
    #[error("actor is deleted")]
    Deleted,
    /// A request contradicts the durable state machine.
    #[error("invalid actor transition: {0}")]
    Invalid(&'static str),
}

impl Error {
    /// Stable, path-free telemetry category. Unknown effects remain unknown.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Call(_) => "call",
            Self::InvocationTerminated { .. } => "invocation_terminated",
            Self::CallSettled { .. } => "call_settled",
            Self::Clock(_) => "clock",
            Self::CompletionStopped => "completion_stopped",
            Self::CompletionFailed(_) => "completion_failed",
            Self::Escrow(_) => "escrow",
            Self::Stream(error) => error.code(),
            Self::Unknown { .. } => "unknown",
            Self::Settlement { .. } => "settlement",
            Self::Conflict => "conflict",
            Self::PreconditionChanged(_) => "precondition_changed",
            Self::PreconditionUnchanged => "precondition_unchanged",
            Self::Paused => "paused",
            Self::Handler(_) => "handler",
            Self::InvocationFailed { .. } => "invocation_failed",
            Self::AlarmNotDue { .. } => "alarm_not_due",
            Self::Deleted => "deleted",
            Self::Invalid(_) => "invalid",
        }
    }
}

/// Alarm identity and deadline are one durable value, including cleared alarms.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Alarm {
    generation: u64,
    deadline_unix_ms: Option<u64>,
    delivered_generation: Option<u64>,
}
impl Alarm {
    /// Generation increases on replacement and clear; Keep preserves it.
    pub const fn generation(&self) -> u64 {
        self.generation
    }
    /// No deadline means cleared; its generation is still retained.
    pub const fn deadline_unix_ms(&self) -> Option<u64> {
        self.deadline_unix_ms
    }
    /// The deadline of an armed, undelivered generation.
    pub fn pending(&self) -> Option<u64> {
        self.deadline_unix_ms
            .filter(|_| self.delivered_generation != Some(self.generation))
    }
}

/// One committed state/cursor/alarm tuple. Fields cannot be forged by callers.
/// Its only durable form is [`State::encode`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    epoch: u64,
    placement_branch: Option<CommitId>,
    escrow: Option<crate::escrow::Escrow>,
    checkpoint: Option<Checkpoint>,
    subscriptions: BTreeMap<String, Subscription>,
    alarm: Alarm,
    paused: bool,
    metadata: BTreeMap<String, bytes::Bytes>,
    journal_tail: u64,
    /// Whether this state's publication appended a participant custody
    /// intent whose settlement a new owner must recover.
    custody: bool,
    /// The alarm this state's own delivery consumed, as `(generation,
    /// deadline)`, independent of the handler's new alarm: the authority's
    /// alarm rule decides the commit from it. Only an alarm delivery sets it.
    consumed: Option<(u64, u64)>,
}
impl State {
    /// Durable guest metadata, from this same checkpoint/epoch/commit cut.
    pub fn metadata(&self) -> impl ExactSizeIterator<Item = (&str, &bytes::Bytes)> {
        self.metadata
            .iter()
            .map(|(key, value)| (key.as_str(), value))
    }
    /// Number of journal frames retained at this same committed state cut.
    pub const fn journal_tail(&self) -> u64 {
        self.journal_tail
    }
    /// Current placement fence. A fork inherits this number until its first
    /// dispatch claims a newer epoch in the original fork publication's branch.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
    /// Last committed artifact checkpoint.
    pub const fn checkpoint(&self) -> Option<&Checkpoint> {
        self.checkpoint.as_ref()
    }
    /// Instance-bound subscriptions, keyed by manifest subscription id.
    pub const fn subscriptions(&self) -> &BTreeMap<String, Subscription> {
        &self.subscriptions
    }
    /// Next unconsumed sequence of a declared subscription.
    pub fn cursor(&self, subscription: &str) -> Option<u64> {
        self.subscriptions
            .get(subscription)
            .map(|value| value.cursor)
    }
    /// Current alarm, including its invalidation generation.
    pub const fn alarm(&self) -> &Alarm {
        &self.alarm
    }
    /// The spend balance this state carries, possibly foreign to the branch
    /// that reads it (an inherited copy cannot spend).
    pub const fn escrow(&self) -> Option<crate::escrow::Escrow> {
        self.escrow
    }
    /// Whether execution is durably paused.
    pub const fn paused(&self) -> bool {
        self.paused
    }
    /// Whether this is an actor's terminal deleted state.
    pub const fn deleted(&self) -> bool {
        self.epoch == DELETED
    }
}

/// The deleted state's epoch. No claim can raise an epoch to it or past it,
/// and no fence admits it, so nothing can follow a deletion.
const DELETED: u64 = u64::MAX;

/// Immutable exact-cut token. Publication compares both tail and commit anchor.
pub struct Snapshot {
    path: StreamPath,
    tail: u64,
    anchor: Option<CommitId>,
    state: Option<State>,
    provenance: Option<Provenance<StreamPath, ForkCut>>,
    /// The cut's committed custody is not known settled. Fixed when the cut
    /// is read or retained, independent of `state`, so a caller that takes
    /// the state cannot drop it: a read cut with custody never knows, and the
    /// receipt of a publication whose composition settled always knows.
    unsettled: bool,
}

struct Publication<'a> {
    trigger: &'a Trigger,
    delivered: Option<&'a Record>,
    transition: Transition,
    key: IdempotencyKey,
}
impl Snapshot {
    fn placement_branch(&self) -> Option<CommitId> {
        self.provenance
            .as_ref()
            .map(|origin| origin.inherited.commit_id)
    }

    /// Whether the last state record was written by the fork commit itself:
    /// a fork overlay, whose funding the forker allocated for this branch.
    /// An inherited record (a copy, including any actor inside an
    /// actor-blind subtree fork) is not, so its funding stays foreign.
    fn overlaid(&self) -> bool {
        self.provenance
            .as_ref()
            .is_some_and(|origin| self.anchor == Some(origin.inherited.commit_id))
    }

    /// The state stream whose committed custody this cut has not seen settled.
    fn unsettled(&self) -> Option<&StreamPath> {
        self.unsettled.then_some(&self.path)
    }

    fn owns_epoch(&self, epoch: u64) -> bool {
        self.state.as_ref().is_some_and(|state| {
            state.epoch == epoch
                && !state.deleted()
                && state.placement_branch == self.placement_branch()
        })
    }

    /// Complete retained tuple, absent before creation.
    pub const fn state(&self) -> Option<&State> {
        self.state.as_ref()
    }
    /// The actor's state stream.
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Follows commits after this exact observation, anchored to its original
    /// history. No second head read is needed to avoid losing a publication.
    pub fn follow_request(&self) -> acyclic_stream::FollowRequest {
        let mut request = acyclic_stream::FollowRequest::new(self.path.clone(), self.tail);
        request.preceding = self.anchor;
        request.when_absent = acyclic_stream::WhenAbsent::Wait;
        request
    }

    /// Original inherited prefix from this same committed observation.
    pub const fn provenance(&self) -> Option<&Provenance<StreamPath, ForkCut>> {
        self.provenance.as_ref()
    }

    fn from_head(path: StreamPath, head: Option<PathHead>) -> Result<Self, Error> {
        let (tail, last, provenance) =
            head.map_or((0, None, None), |head| (head.tail, head.last, head.origin));
        if tail == 0 {
            return Ok(Self {
                path,
                tail,
                anchor: None,
                state: None,
                provenance,
                unsettled: false,
            });
        }
        let record = last.ok_or(Error::Invalid("checkpoint history is incomplete"))?;
        if record.sequence != tail - 1 {
            return Err(Error::Invalid("checkpoint cursor changed"));
        }
        let state = State::decode(&record.value)?;
        Ok(Self {
            path,
            tail,
            anchor: Some(record.commit_id),
            unsettled: state.custody,
            state: Some(state),
            provenance,
        })
    }
}

/// A complete publication the caller may extend with its own conditions and
/// mutations (for example a Streams fork or a filesystem contribution) before
/// submitting it exactly once.
pub struct PreparedPublish {
    own: Option<StreamPath>,
    state: Option<State>,
    call: Option<(crate::call::CallPath, Trigger)>,
    journal: Option<StreamPath>,
    failure_owner: Option<StreamPath>,
    request: CommitRequest,
    composition: Option<Box<dyn Composition>>,
    /// The previous cut's committed, unfinished custody, settled before this
    /// publication can replace it.
    unsettled: Option<StreamPath>,
    sealed: bool,
    acknowledgement_unknown: bool,
    provenance: Option<Provenance<StreamPath, ForkCut>>,
}
impl std::fmt::Debug for PreparedPublish {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedPublish")
            .field("sealed", &self.sealed)
            .field("acknowledgement_unknown", &self.acknowledgement_unknown)
            .field("has_composition", &self.composition.is_some())
            .finish_non_exhaustive()
    }
}
/// A read-only view of a prepared publication's request. It neither clones
/// into nor dereferences to a [`CommitRequest`], so no caller can commit it:
/// a prepared publication commits only through [`Store::submit_retained`],
/// which settles first, or through the guarded [`PreparedPublish::into_request`].
#[derive(Debug)]
pub struct RequestView<'a>(&'a CommitRequest);
impl<'a> RequestView<'a> {
    /// Every mutation, in effect order.
    pub fn mutations(&self) -> impl Iterator<Item = &'a CommitMutation> + 'a {
        self.0.mutations()
    }
}
impl PartialEq<CommitRequest> for RequestView<'_> {
    fn eq(&self, other: &CommitRequest) -> bool {
        self.0 == other
    }
}
impl PreparedPublish {
    /// The request, including the actor's own Head/Absent condition.
    pub const fn request(&self) -> RequestView<'_> {
        RequestView(&self.request)
    }
    /// Join the sealed authored state with its confirmed publication range.
    fn retained(&mut self, receipt: &CommitReceipt) -> Result<Option<Snapshot>, Error> {
        let Some(path) = &self.own else {
            return Ok(None);
        };
        let mut appends = receipt.appends.iter().filter(|append| &append.path == path);
        let append = appends
            .next()
            .ok_or(Error::Invalid("receipt omits actor state"))?;
        let expected = self
            .request
            .effects
            .iter()
            .filter(|effect| !effect.soft)
            .flat_map(|effect| &effect.conditions)
            .find_map(|condition| match condition {
                CommitCondition::Head {
                    path: own,
                    expected,
                    ..
                } if own == path => Some(*expected),
                CommitCondition::Absent { path: own } if own == path => Some(0),
                _ => None,
            });
        let authored = self
            .request
            .effects
            .iter()
            .filter(|effect| !effect.soft)
            .flat_map(|effect| &effect.mutations)
            .find_map(|mutation| match mutation {
                CommitMutation::Append { path: own, records } if own == path => Some(records),
                _ => None,
            });
        let Some([_]) = authored.map(Vec::as_slice) else {
            return Err(Error::Invalid("receipt changes actor state record count"));
        };
        if !self.sealed
            || appends.next().is_some()
            || expected != Some(append.start)
            || append.start.checked_add(1) != Some(append.tail)
        {
            return Err(Error::Invalid("receipt differs from the actor publication"));
        }
        Ok(Some(Snapshot {
            path: path.clone(),
            tail: append.tail,
            anchor: Some(receipt.commit_id),
            state: Some(
                self.state
                    .take()
                    .ok_or(Error::Invalid("publication omitted its authored state"))?,
            ),
            provenance: self.provenance.clone(),
            unsettled: false,
        }))
    }

    /// An empty commit for a caller that is not this actor (a client, or a
    /// parent composing a spawn) that joins companion creates and appends.
    pub fn foreign(key: IdempotencyKey) -> Self {
        Self {
            own: None,
            state: None,
            call: None,
            journal: None,
            failure_owner: None,
            composition: None,
            unsettled: None,
            sealed: false,
            acknowledgement_unknown: false,
            provenance: None,
            request: CommitRequest {
                deadline_unix_millis: None,
                effects: vec![CommitEffect::hard(Vec::new(), Vec::new())],
                idempotency_key: key,
            },
        }
    }
    /// Join admission fences without manufacturing a companion mutation. The
    /// original state fence cannot be weakened, and disagreeing cuts are refused.
    pub fn require(&mut self, required: Vec<CommitCondition>) -> Result<(), Error> {
        if self.sealed {
            return Err(Error::Invalid(
                "sealed actor publication cannot accept conditions",
            ));
        }
        for condition in required {
            let path = CommitCondition::path(&condition);
            if self.own.as_ref() == Some(path) {
                return Err(Error::Invalid("condition targets actor state stream"));
            }
            let conditions = &mut self.request.effects[0].conditions;
            match conditions.binary_search_by(|existing| CommitCondition::path(existing).cmp(path))
            {
                Ok(index) if conditions.get(index) != Some(&condition) => {
                    return Err(Error::Invalid("effects disagree on one stream's cut"));
                }
                Ok(_) => {}
                Err(index) => conditions.insert(index, condition),
            }
        }
        Ok(())
    }
    /// Add one effect. Hard effects join the canonical hard publication;
    /// soft effects apply or skip their entire mutation group together.
    /// Guest effects cannot mutate any runtime-owned stream: the actor's
    /// failure, custody and placement records, or its root's runtime
    /// namespaces (scheduling, dispatcher, sockets).
    pub fn join(&mut self, effect: CommitEffect) -> Result<(), Error> {
        if self
            .own
            .as_ref()
            .or(self.failure_owner.as_ref())
            .is_some_and(|owner| {
                effect
                    .mutations
                    .iter()
                    .any(|mutation| disposition::targets_host_records(mutation, owner))
            })
        {
            return Err(Error::Invalid("effect changes the host's control records"));
        }
        self.join_host(effect)
    }

    // Only the host's failure publisher may contribute reserved failure records.
    fn join_host(&mut self, effect: CommitEffect) -> Result<(), Error> {
        if self.sealed {
            return Err(Error::Invalid(
                "sealed actor publication cannot accept effects",
            ));
        }
        if !effect.admissible() {
            return Err(Error::Invalid("soft effect without mutations"));
        }
        for mutation in &effect.mutations {
            if self
                .journal
                .as_ref()
                .is_some_and(|journal| mutation_targets(mutation, journal))
            {
                return Err(Error::Invalid("effect changes the host's actor journal"));
            }
            if self.call.as_ref().is_some_and(|(call, _)| {
                [call.log(), call.settled()]
                    .into_iter()
                    .any(|path| mutation_targets(mutation, path))
            }) {
                return Err(Error::Invalid("effect changes the host's invocation reply"));
            }
            if self
                .own
                .as_ref()
                .is_some_and(|own| CommitMutation::path(mutation) == own)
            {
                return Err(Error::Invalid("effect targets actor state stream"));
            }
        }
        if effect.soft {
            if self.own.as_ref().is_some_and(|own| {
                effect
                    .conditions
                    .iter()
                    .any(|condition| CommitCondition::path(condition) == own)
            }) {
                return Err(Error::Invalid("condition targets actor state stream"));
            }
            self.request.effects.push(effect);
            return Ok(());
        }
        self.require(effect.conditions)?;
        let mutations = &mut self.request.effects[0].mutations;
        for mutation in effect.mutations {
            match mutation {
                CommitMutation::Append { path, records } => {
                    let existing = mutations.iter_mut().find_map(|mutation| match mutation {
                        CommitMutation::Append {
                            path: existing,
                            records,
                        } if existing == &path => Some(records),
                        _ => None,
                    });
                    match existing {
                        Some(existing) => existing.extend(records),
                        None => mutations.push(CommitMutation::Append { path, records }),
                    }
                }
                fork @ CommitMutation::Fork { .. } => mutations.push(fork),
            }
        }
        Ok(())
    }
    /// Take the request to submit it through another commit owner. Only
    /// [`Store::submit_retained`] settles a participant or the previous cut's
    /// committed custody, so a publication carrying either never leaves.
    pub fn into_request(self) -> Result<CommitRequest, Error> {
        if self.composition.is_some() {
            return Err(Error::Invalid(
                "composed publication must retain participant custody",
            ));
        }
        if self.unsettled.is_some() {
            return Err(Error::Invalid(
                "publication must first settle the committed custody it replaces",
            ));
        }
        Ok(self.request)
    }
    /// Interpret the provider's outcome for this exact request.
    pub const fn settle(outcome: &CommitOutcome) -> Result<(), Error> {
        match outcome {
            CommitOutcome::Committed(_) => Ok(()),
            CommitOutcome::Conflict(_) => Err(Error::Conflict),
        }
    }
}

/// Bind the original caller intent and current fork format before resolving
/// mutable parent/input cuts. It binds the paused, independently funded
/// overlay and the host custody and failure exclusions; the retired
/// `acyclic.actors.fork` acceptances are outside this data generation.
fn fork_key(
    child: &StreamPath,
    request: &crate::wire::ForkActorRequest,
) -> Result<IdempotencyKey, Error> {
    use prost::Message as _;
    use sha2::Digest as _;
    let mut hash = sha2::Sha256::new();
    hash.update(b"acyclic.actor.fork.v1\0");
    hash.update(child.as_str().as_bytes());
    hash.update([0]);
    hash.update(request.encode_to_vec());
    Ok(IdempotencyKey::new(hash.finalize().to_vec())?)
}

/// One storage mechanism for memory, durable local and hosted Stream providers.
pub struct Store<P: StreamProvider + ?Sized> {
    provider: Arc<P>,
    root: StreamPath,
    composer: Option<Arc<dyn Composer>>,
    scheduling_shards: std::num::NonZeroU16,
}

/// The same store with its whole configuration, so a copy cannot disagree
/// with the original on its scheduling partitions or composer.
impl<P: StreamProvider + ?Sized> Clone for Store<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            root: self.root.clone(),
            composer: self.composer.clone(),
            scheduling_shards: self.scheduling_shards,
        }
    }
}
impl<P: StreamProvider + ?Sized> Store<P> {
    /// Retain the caller's admitted provider session. Actor state lives at
    /// `<root>/<encoded actor>`, inside the caller's consistency domain.
    pub fn new(provider: Arc<P>, root: StreamPath) -> Self {
        Self {
            provider,
            root,
            composer: None,
            scheduling_shards: DEFAULT_SCHEDULING_SHARDS,
        }
    }

    /// Open each transition's participant request through `composer`; a
    /// store without one refuses transitions that name a participant.
    #[must_use]
    pub fn with_composer(mut self, composer: Arc<dyn Composer>) -> Self {
        self.composer = Some(composer);
        self
    }

    /// The shared provider session.
    pub fn provider(&self) -> &Arc<P> {
        &self.provider
    }

    /// The state root. Actor records and stable scheduling shards are children
    /// of this root; producers never broadcast wake records on its own stream.
    pub const fn root(&self) -> &StreamPath {
        &self.root
    }

    /// The activation-log append that rides in every state-creating commit,
    /// every resume, and every commit that appends an actor's input. It is
    /// unconditioned, so concurrent creations never contend on it.
    ///
    /// Delivery rests on two invariants. First, every input append commits
    /// with this entry, so a dispatcher replaying its shard from a checkpoint
    /// finds all input. This pairing is NOT enforced: each producer joins the
    /// entry itself, and the stream primitive has no subscription trigger
    /// that would add it. Moving it into the primitive is a tracked
    /// follow-up. Second, an unpaused actor's undelivered work keeps a
    /// carried wake through each later checkpoint, so a claim or crash never
    /// strands it. Only a paused actor's entries pass as finished work, which
    /// is why [`Self::set_paused`] (and any resume) appends this entry again.
    pub fn activation(&self, actor: &ActorReference) -> Result<CommitEffect, Error> {
        let (path, record) = self.activation_record(actor)?;
        Ok(acyclic_stream::CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path,
                records: vec![record.into()],
            }],
        ))
    }

    /// Original wake record for composition with a producer's delivery commit.
    /// Every input append must commit with this record (see
    /// [`Self::activation`]): input appended without it cannot wake a dormant
    /// actor. Nothing enforces the pairing yet; each producer joins it.
    pub fn activation_record(
        &self,
        actor: &ActorReference,
    ) -> Result<(StreamPath, acyclic_stream::OwnedBytes), Error> {
        Ok((
            self.scheduling_path(self.scheduling_shard(actor))?,
            actor.encode().into(),
        ))
    }

    /// The actor's state stream.
    pub fn path(&self, actor: &ActorReference) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/{}",
            self.root,
            acyclic_auth::encode_component(&actor.encode())
        ))?)
    }

    /// Recover the actor address from a direct child of the state root.
    pub fn actor(&self, path: &StreamPath) -> Result<ActorReference, Error> {
        let name = path
            .as_str()
            .strip_prefix(self.root.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
            .filter(|name| !name.contains('/'))
            .ok_or(Error::Invalid(
                "path is not a direct child of the state root",
            ))?;
        let bytes = acyclic_auth::decode_component(name)
            .map_err(|_| Error::Invalid("undecodable actor state name"))?;
        ActorReference::decode(&bytes).map_err(|_| Error::Invalid("undecodable actor reference"))
    }

    /// Serve one `ForkActor` request: branch its parent's current committed
    /// state into its child ([`Self::branch`], born paused with escrow zero)
    /// plus the child's activation, as one commit. Every
    /// server answers the RPC with this one composition (the local host and a
    /// hosted platform alike); `account` is the caller's authenticated
    /// account, used when the request names none. The commit key binds the
    /// child and original request, so retrying an unchanged request recovers
    /// its original version-3 fork before reading current heads. Prior fork
    /// formats require a fresh data generation; no legacy receipt is adopted.
    /// A child that exists
    /// under another intent, or a parent that moves before acceptance, is
    /// [`Error::Conflict`]. Fresh work: one retry-identity lookup, one parent
    /// read, one head per `current_head` subscription, and one commit. Settled
    /// replay: one retry-identity lookup, no head read or mutation dispatch.
    pub async fn fork_actor(
        &self,
        account: &[u8],
        request: &crate::wire::ForkActorRequest,
    ) -> Result<ActorReference, Error> {
        let invalid = || Error::Invalid("invalid fork request");
        let admitted =
            crate::domain::ForkActorRequest::try_from(request.clone()).map_err(|_| invalid())?;
        let parent = admitted.reference(account).map_err(|_| invalid())?;
        let child = admitted.child_reference(account).map_err(|_| invalid())?;
        let parent_path = self.path(&parent)?;
        let child_path = self.path(&child)?;
        let key = fork_key(&child_path, request)?;
        if self.fork_replay(&key, &parent_path, &child_path).await? {
            return Ok(child);
        }
        let mut subscriptions = BTreeMap::new();
        for spec in admitted.subscriptions() {
            let path = StreamPath::new(spec.stream_path().to_owned())?;
            let cursor = match spec.start().cursor_value() {
                Some(cursor) => cursor,
                None => match self.provider.head(path.clone()).await {
                    Ok(head) => head.tail,
                    Err(error) if error.category() == StreamError::NotFound => 0,
                    Err(error) => return Err(error.into()),
                },
            };
            subscriptions.insert(
                spec.subscription_id().to_owned(),
                Subscription { path, cursor },
            );
        }
        let cut = self.load(&parent).await?;
        let mut commit = PreparedPublish::foreign(key.clone());
        commit.join(self.branch(&cut, &child, Branch::unfunded(subscriptions))?)?;
        commit.join(self.activation(&child)?)?;
        match self.submit(commit).await {
            Ok(()) => {}
            Err(
                error @ (Error::Conflict
                | Error::Stream(StreamError::IdempotencyMismatch)
                | Error::Unknown { .. }),
            ) => match self.fork_replay(&key, &parent_path, &child_path).await {
                Ok(true) => {}
                Ok(false) => return Err(error),
                Err(_) if matches!(&error, Error::Unknown { .. }) => return Err(error),
                Err(recovery) => return Err(recovery),
            },
            Err(error) => return Err(error),
        }
        Ok(child)
    }

    // Streams retains the original terminal envelope under this exact intent.
    // Its one fork and one activation are bounded, independent of actor history.
    async fn fork_replay(
        &self,
        key: &IdempotencyKey,
        parent: &StreamPath,
        child: &StreamPath,
    ) -> Result<bool, Error> {
        let Some(observed) = self.provider.inspect_idempotency(key.clone()).await? else {
            return Ok(false);
        };
        if observed.idempotency_key == *key
            && let acyclic_stream::IdempotencyOutcome::Commit(envelope) = observed.outcome
            && envelope.mutations.len() == 2
            && envelope.mutations.iter().any(|mutation| {
                matches!(mutation, acyclic_stream::CommittedMutation::Fork(fork)
                    if fork.source == *parent && fork.destination == *child
                        && disposition::is_fork_exclusion(&fork.exclude))
            })
        {
            return Ok(true);
        }
        Err(Error::Invalid("fork recovery has a foreign publication"))
    }

    /// Read only the final state record; recovery work is independent of history length.
    pub async fn load(&self, actor: &ActorReference) -> Result<Snapshot, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.load",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async { self.load_path(self.path(actor)?).await }
        )
    }

    /// Load several actors at one authority snapshot: one request for any count.
    pub async fn load_all(&self, actors: &[ActorReference]) -> Result<Vec<Snapshot>, Error> {
        let paths = actors
            .iter()
            .map(|actor| self.path(actor))
            .collect::<Result<acyclic_stream::HeadSet<_>, _>>()?;
        let request = acyclic_stream::HeadsRequest {
            retained: Default::default(),
            durable_through: None,
            projection: acyclic_stream::HeadProjection::History,
            limit: u32::try_from(paths.len())
                .map_err(|_| Error::Invalid("too many actors in one load"))?
                .max(1),
            paths,
            children: None,
            at_least: Default::default(),
        };
        let view = self.provider.heads(request.clone()).await?;
        view.validate_for(&request)?;
        request
            .paths
            .into_iter()
            .zip(view.heads)
            .map(|(path, head)| Snapshot::from_head(path, head))
            .collect()
    }

    async fn load_path(&self, path: StreamPath) -> Result<Snapshot, Error> {
        let head = self.head(&path).await?;
        Snapshot::from_head(path, head)
    }

    /// Observe the original path once. None confirms its tail and commit anchor
    /// were unchanged at this observation, without decoding the retained state
    /// again. A changed cut is decoded. This is not permission to start external
    /// effects after a later control change; publication still uses its original
    /// exact-head/epoch fence.
    pub async fn observe(&self, previous: &Snapshot) -> Result<Option<Snapshot>, Error> {
        let head = self.head(previous.path()).await?;
        let tail = head.as_ref().map_or(0, |head| head.tail);
        let anchor = head
            .as_ref()
            .filter(|head| head.tail != 0)
            .and_then(|head| head.last.as_ref())
            .map(|record| record.commit_id);
        if tail == previous.tail && anchor == previous.anchor {
            return Ok(None);
        }
        Snapshot::from_head(previous.path.clone(), head).map(Some)
    }

    async fn head(&self, path: &StreamPath) -> Result<Option<PathHead>, Error> {
        match self.provider.head(path.clone()).await {
            Ok(head) => Ok(Some(head)),
            Err(error) if error.category() == StreamError::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Establish a strictly newer epoch while preserving checkpoint, cursors,
    /// alarm and pause state. Returns the receipt-backed cut without a reread.
    pub async fn claim(
        &self,
        previous: &Snapshot,
        epoch: u64,
        key: IdempotencyKey,
    ) -> Result<Snapshot, Error> {
        self.claim_with_pause(previous, epoch, None, key).await
    }

    // The original conditional placement publication can also decide control
    // for a fork that cannot dispatch while it inherits a paused state.
    async fn claim_with_pause(
        &self,
        previous: &Snapshot,
        epoch: u64,
        paused: Option<bool>,
        key: IdempotencyKey,
    ) -> Result<Snapshot, Error> {
        self.claim_control(previous, epoch, paused, Vec::new(), key)
            .await
    }

    /// Settle the participant of the claimed state's composed publication
    /// again. Its old owner may have died between that publication and its
    /// settlement; the new owner's claim fenced the old owner's publications.
    /// Settlement is therefore at least once across owners and exactly once
    /// within one, so every composer settles idempotently. A state without
    /// custody, or one this process settled, costs nothing here.
    pub(crate) async fn recover_custody(&self, snapshot: &Snapshot) -> Result<(), Error> {
        match snapshot.unsettled() {
            Some(path) => self.settle_custody(path).await,
            None => Ok(()),
        }
    }

    async fn settle_custody(&self, path: &StreamPath) -> Result<(), Error> {
        let composer = self
            .composer
            .as_ref()
            .ok_or(Error::Invalid("actor custody has no composer"))?;
        let intent = record::Intent::decode(
            &self
                .provider
                .head(custody_path(path)?)
                .await?
                .last
                .ok_or(Error::Invalid("actor custody intent is missing"))?
                .value,
        )?;
        let observation = self
            .provider
            .inspect_idempotency(intent.key.clone())
            .await?
            .ok_or(Error::Invalid(
                "composed publication outcome is not retained",
            ))?;
        let (acyclic_stream::IdempotencyOutcome::Commit(envelope), Some(revision)) =
            (observation.outcome, observation.revision)
        else {
            return Err(Error::Invalid(
                "composed publication outcome is not a commit",
            ));
        };
        let own = envelope
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                acyclic_stream::CommittedMutation::Append(append) if &append.path == path => {
                    Some(append)
                }
                _ => None,
            })
            .ok_or(Error::Invalid("composed publication omits actor state"))?;
        let next = match own.records.as_slice() {
            [record] => State::decode(&record.value)?
                .checkpoint
                .ok_or(Error::Invalid("composed publication has no checkpoint"))?,
            _ => {
                return Err(Error::Invalid(
                    "composed publication changes state record count",
                ));
            }
        };
        // (path, seq) is immutable; proof: custody_recovery_never_reads_a_replaced_input.
        let previous = match own.start.checked_sub(1) {
            Some(sequence) => State::decode(&self.record(path, sequence).await?.value)?.checkpoint,
            None => None,
        };
        let delivered = match &intent.delivered {
            Some((path, sequence)) => Some(self.record(path, *sequence).await?),
            None => None,
        };
        let appends = envelope
            .mutations
            .iter()
            .filter(|mutation| matches!(mutation, acyclic_stream::CommittedMutation::Append(_)))
            .count();
        let receipt = CommitReceipt::project(
            &envelope,
            revision,
            Vec::with_capacity(appends),
            Vec::with_capacity(envelope.mutations.len() - appends),
            |_, _| Ok::<(), StreamError>(()),
        )?;
        composer
            .open(
                &self.actor(path)?,
                previous.as_ref(),
                delivered.as_ref(),
                &next,
                &intent.participant,
            )
            .map_err(Error::Handler)?
            .settle(Ok(&CommitOutcome::Committed(receipt)))
            .await
            .map_err(Error::Handler)
    }

    /// The one record at `sequence` of `path`.
    async fn record(&self, path: &StreamPath, sequence: u64) -> Result<Record, Error> {
        use futures::StreamExt as _;
        self.provider
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: path.clone(),
                from: sequence,
                limit: 1,
                at_least: Default::default(),
            })
            .await?
            .next()
            .await
            .transpose()?
            .filter(|record| record.sequence == sequence)
            .ok_or(Error::Invalid("custody names a missing record"))
    }

    async fn claim_control(
        &self,
        previous: &Snapshot,
        epoch: u64,
        paused: Option<bool>,
        effects: Vec<CommitEffect>,
        key: IdempotencyKey,
    ) -> Result<Snapshot, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.claim",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let mut state = previous
                    .state
                    .clone()
                    .ok_or(Error::Invalid("actor state was never created"))?;
                if epoch <= state.epoch {
                    return Err(Error::Conflict);
                }
                state.epoch = epoch;
                state.placement_branch = previous.placement_branch();
                if previous.overlaid() {
                    state.escrow = state
                        .escrow
                        .map(|escrow| escrow.in_branch(state.placement_branch));
                }
                if let Some(paused) = paused {
                    state.paused = paused;
                }
                let mut prepared = self.prepare(previous, state, Vec::new(), key, false)?;
                for effect in effects {
                    prepared.join_host(effect)?;
                }
                self.submit_retained(prepared)
                    .await?
                    .ok_or(Error::Invalid("claim receipt has no state"))
            }
        )
    }

    /// Pause/resume and in-flight publication share the identical CAS fence.
    /// An unclaimed actor (epoch 0) can be paused before its first placement.
    pub async fn set_paused(
        &self,
        previous: &Snapshot,
        epoch: u64,
        paused: bool,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.set_paused",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let mut state = previous
                    .state
                    .as_ref()
                    .filter(|_| previous.owns_epoch(epoch))
                    .ok_or(Error::Conflict)?
                    .clone();
                state.paused = paused;
                self.submit(self.prepare(previous, state, Vec::new(), key, false)?)
                    .await
            }
        )
    }

    /// Delete the actor at `previous`'s exact cut: one terminal state record
    /// at its own path, which callers may compose with their own effects
    /// before submitting. It fences every in-flight claim and publication of
    /// that cut, keeps the path occupied so the actor is never re-created,
    /// and carries no input, alarm, checkpoint or funds, so scheduling entries
    /// that still name it resolve to a no-op. A pending participant's custody
    /// is settled first, as before every publication.
    pub fn delete(
        &self,
        previous: &Snapshot,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        let state = previous
            .state
            .as_ref()
            .ok_or(Error::Invalid("actor state was never created"))?;
        if state.deleted() {
            return Err(Error::Deleted);
        }
        let deleted = State {
            epoch: DELETED,
            placement_branch: state.placement_branch,
            escrow: None,
            checkpoint: None,
            subscriptions: BTreeMap::new(),
            alarm: Alarm {
                generation: state
                    .alarm
                    .generation
                    .checked_add(1)
                    .ok_or(Error::Invalid("alarm generation overflow"))?,
                deadline_unix_ms: None,
                delivered_generation: None,
            },
            paused: true,
            metadata: BTreeMap::new(),
            journal_tail: state.journal_tail,
            custody: false,
            consumed: None,
        };
        self.prepare(previous, deleted, Vec::new(), key, false)
    }

    /// Atomically publish the checkpoint, consumed cursor, alarm and effects.
    /// `delivered` is the committed record `trigger` names, as the host read it.
    pub async fn publish(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        delivered: Option<&Record>,
        transition: Transition,
        key: IdempotencyKey,
    ) -> Result<(), Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.publish",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let mut transition = transition;
                self.resolve_starts(&mut transition).await?;
                self.submit(
                    self.prepare_publish(previous, epoch, trigger, delivered, transition, key)?,
                )
                .await
            }
        )
    }

    /// Bind each head-start subscription in `transition` to its stream's
    /// current tail: one head read per such subscription, before publication,
    /// so every record committed after the publication is delivered. A stream
    /// that does not exist yet starts at zero.
    pub async fn resolve_starts(&self, transition: &mut Transition) -> Result<(), Error> {
        for change in &mut transition.subscriptions {
            if let crate::guest::SubscriptionChange::Add {
                path,
                from: from @ None,
                ..
            } = change
            {
                *from = Some(self.head(path).await?.map_or(0, |head| head.tail));
            }
        }
        Ok(())
    }

    /// Build the one publication without submitting it.
    pub fn prepare_publish(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        delivered: Option<&Record>,
        transition: Transition,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        let state = owned(previous, epoch)?;
        self.prepare_publication(
            previous,
            state.clone(),
            &state.alarm,
            Publication {
                trigger,
                delivered,
                transition,
                key,
            },
        )
    }

    fn prepare_publication(
        &self,
        previous: &Snapshot,
        mut state: State,
        original_alarm: &Alarm,
        publication: Publication<'_>,
    ) -> Result<PreparedPublish, Error> {
        let Publication {
            trigger,
            delivered,
            transition,
            key,
        } = publication;
        let named = match trigger {
            Trigger::Record { sequence, .. } => Some(*sequence),
            Trigger::Alarm { .. } => None,
        };
        if let Some(record) = delivered
            && named != Some(record.sequence)
        {
            return Err(Error::Invalid("delivered record differs from its trigger"));
        }
        // The call's completion is a host record, joined past the guest check.
        let (call, completion) = match self.invocation_completion(
            previous,
            trigger,
            delivered,
            transition.response.as_ref().map(Ok),
            &transition.effects,
        )? {
            Some((path, completion)) => (Some(path), completion),
            None => (None, Vec::new()),
        };
        let previous_checkpoint = state.checkpoint.take();
        // A participant's custody intent rides in the publication itself.
        let intent = match &transition.participant {
            None => None,
            Some(participant) => Some(record::Intent {
                participant: participant.clone(),
                key: key.clone(),
                delivered: match (trigger, delivered) {
                    (Trigger::Record { subscription, .. }, Some(record)) => Some((
                        state
                            .subscriptions
                            .get(subscription)
                            .ok_or(Error::Invalid("trigger names no subscription"))?
                            .path
                            .clone(),
                        record.sequence,
                    )),
                    _ => None,
                },
            }),
        };
        let state = next(state, &previous.path, original_alarm, trigger, &transition)?;
        // The participant is a request from a possibly untrusted handler:
        // the host composer validates it before any custody exists.
        let composition = match &transition.participant {
            None => None,
            Some(participant) => Some(
                self.composer
                    .as_ref()
                    .ok_or(Error::Invalid(
                        "transition names a participant without a composer",
                    ))?
                    .open(
                        &self.actor(&previous.path)?,
                        previous_checkpoint.as_ref(),
                        delivered,
                        &transition.checkpoint,
                        participant,
                    )?,
            ),
        };
        let mut prepared =
            self.prepare(previous, state, transition.effects, key, intent.is_some())?;
        for effect in completion {
            prepared.join_host(effect)?;
        }
        if let Some(intent) = intent {
            prepared.join_host(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: custody_path(&previous.path)?,
                    records: vec![intent.encode().into()],
                }],
            ))?;
        }
        prepared.call = call.map(|call| (call, trigger.clone()));
        prepared.composition = composition;
        Ok(prepared)
    }

    /// Seal and submit once. An unknown acknowledgement returns the whole
    /// publication in [`Error::Unknown`]; retries preserve its bytes and custody.
    pub async fn submit(&self, prepared: PreparedPublish) -> Result<(), Error> {
        self.submit_retained(prepared).await.map(|_| ())
    }

    /// Submit once and return the exact authored state confirmed by the original
    /// receipt. Foreign publications return None. No post-write read or decode
    /// is needed. Unknown outcomes retain the entire publication in Error::Unknown.
    pub async fn submit_retained(
        &self,
        mut prepared: PreparedPublish,
    ) -> Result<Option<Snapshot>, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.submit",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                // Committed custody is durable state: every publication from
                // its cut, a retry by the same owner included, finishes it
                // first, and a failure leaves it committed for the next one.
                if let Some(path) = &prepared.unsettled {
                    self.settle_custody(path).await?;
                    prepared.unsettled = None;
                }
                if !prepared.sealed {
                    if let Some(composition) = &mut prepared.composition {
                        // Compare canonical requests: a participant's own
                        // validation may reorder equivalent fork exclusions.
                        acyclic_stream::request::validate_commit(&mut prepared.request)?;
                        let original = &prepared.request;
                        let request = composition.seal(original.clone()).await.map_err(
                            |error| match error {
                                GuestError::PreconditionChanged => {
                                    Error::PreconditionChanged(Vec::new())
                                }
                                error => Error::Handler(error),
                            },
                        )?;
                        if !extends(original, &request)
                            || prepared.own.as_ref().is_some_and(|own| {
                                request.mutations().filter(|mutation| {
                                    matches!(mutation, CommitMutation::Append { path, .. } if path == own)
                                }).count() != 1
                            })
                            || prepared.call.as_ref().is_some_and(|(call, _)| {
                                [call.log(), call.settled()].into_iter().any(|protected| {
                                    request.mutations().filter(|mutation| mutation_targets(mutation, protected)).count()
                                        != original.mutations().filter(|mutation| mutation_targets(mutation, protected)).count()
                                })
                            })
                            || prepared.journal.as_ref().is_some_and(|journal| {
                                request.mutations().filter(|mutation| mutation_targets(mutation, journal)).count()
                                    != original.mutations().filter(|mutation| mutation_targets(mutation, journal)).count()
                            })
                            || prepared
                                .own
                                .as_ref()
                                .or(prepared.failure_owner.as_ref())
                                .is_some_and(|owner| {
                                    request.mutations()
                                        .filter(|mutation| {
                                            disposition::targets_host_records(mutation, owner)
                                        })
                                        .count()
                                        != original.mutations()
                                            .filter(|mutation| {
                                                disposition::targets_host_records(mutation, owner)
                                            })
                                            .count()
                                })
                        {
                            return Err(Error::Invalid("composition changed the actor publication"));
                        }
                        prepared.request = request;
                    }
                    prepared.sealed = true;
                }
                let outcome = self.provider.commit(prepared.request.clone()).await;
                // A retry refusal cannot undo an earlier attempt that may have applied.
                if prepared.acknowledgement_unknown {
                    if let Err(source) = &outcome {
                        return Err(Error::Unknown {
                            publication: Box::new(prepared),
                            source: source.clone(),
                        });
                    }
                }
                if let Err(source) = &outcome
                    && matches!(
                        source.category(),
                        StreamError::Unavailable | StreamError::InvalidResponse
                    )
                {
                    prepared.acknowledgement_unknown = true;
                    return Err(Error::Unknown {
                        publication: Box::new(prepared),
                        source: source.clone(),
                    });
                }
                let retained = match &outcome {
                    Ok(CommitOutcome::Committed(receipt)) => prepared.retained(receipt),
                    Ok(CommitOutcome::Conflict(_)) | Err(_) => Ok(None),
                };
                let revalidate = match (&outcome, &prepared.composition) {
                    (Ok(CommitOutcome::Conflict(conflicts)), Some(composition)) => {
                        conflicts.iter().all(|conflict| {
                            let path = match conflict {
                                acyclic_stream::CommitConflict::Missing { path }
                                | acyclic_stream::CommitConflict::Head { path, .. }
                                | acyclic_stream::CommitConflict::Tail { path, .. }
                                | acyclic_stream::CommitConflict::Exists { path } => path,
                            };
                            prepared.own.as_ref() != Some(path)
                        }) && composition.revalidate(conflicts)
                    }
                    _ => false,
                };
                let settled = prepared.call.filter(|(call, _)| matches!(&outcome,
                    Ok(CommitOutcome::Conflict(conflicts)) if !conflicts.is_empty()
                        && conflicts.iter().all(|conflict| matches!(conflict,
                            acyclic_stream::CommitConflict::Exists { path } if path == call.settled()))));
                if let Some(composition) = prepared.composition {
                    if let Err(source) = composition.settle(outcome.as_ref()).await {
                        return Err(Error::Settlement { outcome, source });
                    }
                }
                if let Some((call, trigger)) = settled {
                    return Err(Error::CallSettled { call, trigger });
                }
                if revalidate {
                    return Err(Error::PreconditionChanged(match outcome {
                        Ok(CommitOutcome::Conflict(conflicts)) => conflicts,
                        _ => Vec::new(),
                    }));
                }
                if let Err(source) = &outcome
                    && let Some(authority_unix_ms) = source.not_before_authority()
                {
                    return Err(Error::AlarmNotDue { authority_unix_ms });
                }
                PreparedPublish::settle(&outcome?)?;
                retained
            }
        )
    }

    /// The one constructor of every state write. Committed custody is
    /// settled before the write commits (see [`Self::submit_retained`]), so the
    /// new state holds custody only when this write composes a participant:
    /// `composed`, never a copy of the previous state's marker.
    fn prepare(
        &self,
        previous: &Snapshot,
        mut state: State,
        effects: Vec<CommitEffect>,
        key: IdempotencyKey,
        composed: bool,
    ) -> Result<PreparedPublish, Error> {
        state.custody = composed;
        let (own, created) = match previous.anchor {
            Some(commit_id) => (
                CommitCondition::Head {
                    path: previous.path.clone(),
                    expected: previous.tail,
                    commit_id,
                },
                None,
            ),
            None => (
                CommitCondition::Absent {
                    path: previous.path.clone(),
                },
                Some(self.activation(&self.actor(&previous.path)?)?),
            ),
        };
        // Arming (or inheriting into an own write) a deadline files the actor
        // in its shard's deadline window in this same commit, so a restarted host
        // finds every pending alarm without reading actor states.
        let armed = state
            .alarm
            .pending()
            .filter(|deadline| {
                previous.overlaid()
                    || previous.state().and_then(|state| state.alarm.pending()) != Some(*deadline)
            })
            .map(|deadline| self.deadline(&self.actor(&previous.path)?, deadline))
            .transpose()?;
        // A resume is the one transition that makes committed input
        // deliverable after the dispatcher finished it: a paused actor's
        // activation entries and alarm window pass without a carried wake.
        // So every resume appends the actor's activation in the same commit;
        // its in-memory wake dies with the host, and a restart replays this
        // entry until delivery holds the input. (A claim needs none: an
        // unpaused actor's undelivered input keeps a carried wake.)
        let deliverable = (created.is_none()
            && !state.paused
            && previous.state().is_some_and(|before| before.paused))
        .then(|| self.activation(&self.actor(&previous.path)?))
        .transpose()?;
        let encoded = state.encode();
        let mut prepared = PreparedPublish {
            own: Some(previous.path.clone()),
            state: Some(state),
            call: None,
            journal: None,
            failure_owner: None,
            composition: None,
            unsettled: previous.unsettled().cloned(),
            sealed: false,
            acknowledgement_unknown: false,
            provenance: previous.provenance.clone(),
            request: CommitRequest {
                deadline_unix_millis: None,
                effects: vec![CommitEffect::hard(
                    vec![own],
                    vec![CommitMutation::Append {
                        path: previous.path.clone(),
                        records: vec![encoded.into()],
                    }],
                )],
                idempotency_key: key,
            },
        };
        for effect in created.into_iter().chain(armed).chain(deliverable) {
            prepared.join_host(effect)?;
        }
        for effect in effects {
            prepared.join(effect)?;
        }
        Ok(prepared)
    }
}

fn custody_path(state: &StreamPath) -> Result<StreamPath, Error> {
    Ok(StreamPath::new(format!(
        "{state}/{}",
        disposition::CUSTODY
    ))?)
}

fn covers(ancestor: &StreamPath, path: &StreamPath) -> bool {
    ancestor == path
        || path
            .as_str()
            .strip_prefix(ancestor.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

/// Whether `mutation` writes `path`: a fork writes its whole subtree.
fn mutation_targets(mutation: &CommitMutation, path: &StreamPath) -> bool {
    match mutation {
        CommitMutation::Fork { destination, .. } => covers(destination, path),
        mutation => mutation.path() == path,
    }
}

/// Whether a composed request keeps the original publication: every soft
/// effect unchanged, and every hard condition and mutation in a hard effect.
fn extends(original: &CommitRequest, composed: &CommitRequest) -> bool {
    let hard = || composed.effects.iter().filter(|effect| !effect.soft);
    original.effects.iter().all(|effect| {
        if effect.soft {
            return composed.effects.contains(effect);
        }
        effect
            .conditions
            .iter()
            .all(|entry| hard().any(|hard| hard.conditions.contains(entry)))
            && effect
                .mutations
                .iter()
                .all(|entry| hard().any(|hard| hard.mutations.contains(entry)))
    })
}

fn owned(previous: &Snapshot, epoch: u64) -> Result<&State, Error> {
    previous
        .state
        .as_ref()
        .filter(|_| epoch != 0 && previous.owns_epoch(epoch))
        .ok_or(Error::Conflict)
}

/// The pure durable transition: consume exactly the trigger, then apply the
/// alarm change. Checkpoint, cursor and alarm move together or not at all.
fn next(
    mut state: State,
    own: &StreamPath,
    original_alarm: &Alarm,
    trigger: &Trigger,
    transition: &Transition,
) -> Result<State, Error> {
    consume(&mut state, original_alarm, trigger)?;
    change_alarm(&mut state.alarm, transition.alarm)?;
    subscribe(&mut state.subscriptions, own, &transition.subscriptions)?;
    state.checkpoint = Some(transition.checkpoint.clone());
    Ok(state)
}

/// Apply the actor's own subscription changes in order. A start the host has
/// not resolved is refused, so every bound cursor is an explicit sequence.
fn subscribe(
    subscriptions: &mut BTreeMap<String, Subscription>,
    own: &StreamPath,
    changes: &[crate::guest::SubscriptionChange],
) -> Result<(), Error> {
    use crate::guest::SubscriptionChange;
    for change in changes {
        match change {
            SubscriptionChange::Add { id, path, from } => {
                if id.is_empty() {
                    return Err(Error::Invalid("subscription has no id"));
                }
                if path == own {
                    return Err(Error::Invalid("actor subscribes to its own state"));
                }
                let cursor = from.ok_or(Error::Invalid("subscription start is unresolved"))?;
                match subscriptions.get(id) {
                    Some(bound) if &bound.path == path => {}
                    Some(_) => return Err(Error::Invalid("subscription id is bound elsewhere")),
                    None => {
                        subscriptions.insert(
                            id.clone(),
                            Subscription {
                                path: path.clone(),
                                cursor,
                            },
                        );
                    }
                }
            }
            SubscriptionChange::Remove { id } => {
                subscriptions.remove(id);
            }
        }
    }
    Ok(())
}

fn consume(state: &mut State, original_alarm: &Alarm, trigger: &Trigger) -> Result<(), Error> {
    if state.paused {
        return Err(Error::Paused);
    }
    match trigger {
        Trigger::Record {
            subscription,
            sequence,
        } => {
            let cursor = &mut state
                .subscriptions
                .get_mut(subscription)
                .ok_or(Error::Invalid("undeclared subscription"))?
                .cursor;
            if *cursor != *sequence {
                return Err(Error::Conflict);
            }
            *cursor = cursor
                .checked_add(1)
                .ok_or(Error::Invalid("subscription cursor overflow"))?;
            state.consumed = None;
        }
        Trigger::Alarm {
            generation,
            now_unix_ms,
        } => {
            let deadline = original_alarm
                .pending()
                .filter(|deadline| {
                    original_alarm.generation == *generation && *deadline <= *now_unix_ms
                })
                .ok_or(Error::Conflict)?;
            // The authority decides this commit from the consumed alarm
            // itself, whatever the handler answers.
            state.consumed = Some((*generation, deadline));
            // Delivery consumption is distinct from the desired alarm.
            // Keep preserves its generation/deadline without permitting a
            // second successful firing to publish another transition.
            if state.alarm.generation == *generation {
                state.alarm.delivered_generation = Some(*generation);
            }
        }
    }
    Ok(())
}

fn change_alarm(alarm: &mut Alarm, change: AlarmChange) -> Result<(), Error> {
    let deadline = match change {
        AlarmChange::Keep => None,
        AlarmChange::Clear => Some(None),
        AlarmChange::Set {
            deadline_unix_ms: 0,
        } => return Err(Error::Invalid("alarm deadline is zero")),
        AlarmChange::Set { deadline_unix_ms } => Some(Some(deadline_unix_ms)),
    };
    if let Some(deadline) = deadline {
        alarm.generation = alarm
            .generation
            .checked_add(1)
            .ok_or(Error::Invalid("alarm generation overflow"))?;
        alarm.deadline_unix_ms = deadline;
        alarm.delivered_generation = None;
    }
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod model;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
