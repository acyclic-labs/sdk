//! Single-node Actors runtime primitives over the canonical Stream transaction.
mod applied;
mod state_binding;
pub use state_binding::{ActorStateBinding, ActorStateTransaction};
mod invoke;
pub use invoke::{CommittedInvocation, InvocationReceipt, InvocationTicket};
/// Shared readiness registration and hibernation handoff.
pub mod ready;
mod record;
pub use applied::{Applied, AppliedCut};
pub use record::{METADATA_OVERHEAD, STATE_RECORD_OVERHEAD, SUBSCRIPTION_OVERHEAD};
mod runtime;
pub use crate::guest::Subscription;
pub use runtime::{Clock, Runtime, RuntimeLimits, SystemClock};

use crate::addressing::ActorReference;
use crate::guest::{
    AlarmChange, Checkpoint, Composer, Composition, Effect, GuestError, Transition, Trigger,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitReceipt, CommitRequest,
    IdempotencyKey, PathHead, Record, StreamError, StreamPath, StreamProvider,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Typed failure at the shared actor publication boundary.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    /// A request contradicts the durable state machine.
    #[error("invalid actor transition: {0}")]
    Invalid(&'static str),
}

impl Error {
    /// Stable, path-free telemetry category. Unknown effects remain unknown.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Stream(error) => error.code(),
            Self::Unknown { .. } => "unknown",
            Self::Settlement { .. } => "settlement",
            Self::Conflict => "conflict",
            Self::Paused => "paused",
            Self::Handler(_) => "handler",
            Self::InvocationFailed { .. } => "invocation_failed",
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
    checkpoint: Option<Checkpoint>,
    subscriptions: BTreeMap<String, Subscription>,
    alarm: Alarm,
    paused: bool,
    metadata: BTreeMap<String, bytes::Bytes>,
    journal_tail: u64,
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
    /// Current placement fence; zero means created but never placed.
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
    /// Whether execution is durably paused.
    pub const fn paused(&self) -> bool {
        self.paused
    }
}

/// Immutable exact-cut token. Publication compares both tail and commit anchor.
pub struct Snapshot {
    path: StreamPath,
    tail: u64,
    anchor: Option<CommitId>,
    state: Option<State>,
}

struct Publication<'a> {
    trigger: &'a Trigger,
    delivered: Option<&'a Record>,
    transition: Transition,
    key: IdempotencyKey,
}
impl Snapshot {
    /// Complete retained tuple, absent before creation.
    pub const fn state(&self) -> Option<&State> {
        self.state.as_ref()
    }
    /// The actor's state stream.
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    fn from_head(path: StreamPath, head: Option<PathHead>) -> Result<Self, Error> {
        let (tail, last) = head.map_or((0, None), |head| (head.tail, head.last));
        if tail == 0 {
            return Ok(Self {
                path,
                tail,
                anchor: None,
                state: None,
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
            state: Some(state),
        })
    }
}

/// A complete publication the caller may extend with its own conditions and
/// mutations (for example a Streams fork or a filesystem contribution) before
/// submitting it exactly once.
pub struct PreparedPublish {
    own: Option<StreamPath>,
    state: Option<State>,
    reply: Option<StreamPath>,
    journal: Option<StreamPath>,
    request: CommitRequest,
    composition: Option<Box<dyn Composition>>,
    sealed: bool,
    acknowledgement_unknown: bool,
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
impl PreparedPublish {
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
            .conditions
            .iter()
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
            .mutations
            .iter()
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
        }))
    }

    /// An empty commit for a caller that is not this actor (a client, or a
    /// parent composing a spawn) that joins companion creates and appends.
    pub fn foreign(key: IdempotencyKey) -> Self {
        Self {
            own: None,
            state: None,
            reply: None,
            journal: None,
            composition: None,
            sealed: false,
            acknowledgement_unknown: false,
            request: CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: Vec::new(),
                idempotency_key: key,
            },
        }
    }
    /// The request, including the actor's own Head/Absent condition.
    pub const fn request(&self) -> &CommitRequest {
        &self.request
    }
    /// Add one participant, keeping conditions canonical (one per path, sorted)
    /// and merging appends to the same path in order. The actor's own state
    /// stream cannot be named; disagreeing cuts on one path are refused.
    pub fn join(&mut self, effect: Effect) -> Result<(), Error> {
        if self.sealed {
            return Err(Error::Invalid(
                "sealed actor publication cannot accept effects",
            ));
        }
        if self
            .journal
            .as_ref()
            .is_some_and(|journal| mutation_targets(&effect.mutation, journal))
        {
            return Err(Error::Invalid("effect changes the host's actor journal"));
        }
        if self
            .reply
            .as_ref()
            .is_some_and(|reply| match &effect.mutation {
                CommitMutation::Append { path, .. } => path == reply,
                CommitMutation::Fork { destination, .. } => covers(destination, reply),
            })
        {
            return Err(Error::Invalid("effect changes the host's invocation reply"));
        }
        let Self { own, request, .. } = self;
        let touches_own = |path: &StreamPath| own.as_ref() == Some(path);
        for condition in effect.conditions {
            let path = condition_path(&condition);
            if touches_own(path) {
                return Err(Error::Invalid("effect targets actor state stream"));
            }
            let conditions = &mut request.conditions;
            match conditions.binary_search_by(|existing| condition_path(existing).cmp(path)) {
                Ok(index) if conditions.get(index) != Some(&condition) => {
                    return Err(Error::Invalid("effects disagree on one stream's cut"));
                }
                Ok(_) => {}
                Err(index) => conditions.insert(index, condition),
            }
        }
        let mutations = &mut request.mutations;
        match effect.mutation {
            CommitMutation::Append { path, records } => {
                if touches_own(&path) {
                    return Err(Error::Invalid("effect targets actor state stream"));
                }
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
            fork @ CommitMutation::Fork { .. } => {
                if let CommitMutation::Fork { destination, .. } = &fork
                    && touches_own(destination)
                {
                    return Err(Error::Invalid("effect targets actor state stream"));
                }
                mutations.push(fork);
            }
        }
        Ok(())
    }
    /// Take the request to submit it through another commit owner.
    pub fn into_request(self) -> Result<CommitRequest, Error> {
        if self.composition.is_some() {
            return Err(Error::Invalid(
                "composed publication must retain participant custody",
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

/// One storage mechanism for memory, durable local and hosted Stream providers.
pub struct Store<P: StreamProvider + ?Sized> {
    provider: Arc<P>,
    root: StreamPath,
    composer: Option<Arc<dyn Composer>>,
}
impl<P: StreamProvider + ?Sized> Store<P> {
    /// Retain the caller's admitted provider session. Actor state lives at
    /// `<root>/<encoded actor>`, inside the caller's consistency domain.
    pub fn new(provider: Arc<P>, root: StreamPath) -> Self {
        Self {
            provider,
            root,
            composer: None,
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

    /// The state root. Its own stream is the activation log: every commit that
    /// creates actor state appends that actor's address to it.
    pub const fn root(&self) -> &StreamPath {
        &self.root
    }

    /// The activation-log append that rides in every state-creating commit.
    /// It is unconditioned, so concurrent creations never contend on it.
    pub fn activation(&self, actor: &ActorReference) -> Effect {
        Effect {
            conditions: Vec::new(),
            mutation: CommitMutation::Append {
                path: self.root.clone(),
                records: vec![actor.encode().into()],
            },
        }
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

    /// The companion effects that create an unplaced (epoch 0) actor inside a
    /// caller-built commit, such as a spawn: its state stream and its
    /// activation-log entry. Its cursors are the first records the actor will
    /// consume; a later `claim` preserves them. Create each subscribed stream
    /// in the same commit (or earlier): an absent stream is no input.
    pub fn create(
        &self,
        actor: &ActorReference,
        subscriptions: BTreeMap<String, Subscription>,
        checkpoint: Option<Checkpoint>,
    ) -> Result<[Effect; 2], Error> {
        let path = self.path(actor)?;
        if subscriptions.values().any(|value| value.path == path) {
            return Err(Error::Invalid("actor subscribes to its own state"));
        }
        let state = State {
            epoch: 0,
            checkpoint,
            subscriptions,
            alarm: Alarm::default(),
            paused: false,
            metadata: BTreeMap::new(),
            journal_tail: 0,
        };
        Ok([
            Effect {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutation: CommitMutation::Append {
                    path,
                    records: vec![state.encode().into()],
                },
            },
            self.activation(actor),
        ])
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
            Err(StreamError::NotFound) => Ok(None),
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
                self.submit_retained(self.prepare(previous, state, Vec::new(), key)?)
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
                    .filter(|state| state.epoch == epoch)
                    .ok_or(Error::Conflict)?
                    .clone();
                state.paused = paused;
                self.submit(self.prepare(previous, state, Vec::new(), key)?)
                    .await
            }
        )
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
                self.submit(
                    self.prepare_publish(previous, epoch, trigger, delivered, transition, key)?,
                )
                .await
            }
        )
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
            mut transition,
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
        let reply = if let Some((path, reply)) = self.invocation_reply(
            previous,
            trigger,
            delivered,
            transition.response.as_ref().map(Ok),
            &transition.effects,
        )? {
            transition.effects.push(reply);
            Some(path)
        } else {
            None
        };
        let previous_checkpoint = state.checkpoint.take();
        let state = next(state, original_alarm, trigger, &transition)?;
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
        let mut prepared = self.prepare(previous, state, transition.effects, key)?;
        prepared.reply = reply;
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
                if !prepared.sealed {
                    if let Some(composition) = &mut prepared.composition {
                        let original = &prepared.request;
                        let request = composition.seal(original.clone()).await?;
                        if !original
                    .conditions
                    .iter()
                    .all(|entry| request.conditions.contains(entry))
                    || !original
                        .mutations
                        .iter()
                        .all(|entry| request.mutations.contains(entry))
                    || prepared.own.as_ref().is_some_and(|own| {
                        request.mutations.iter().filter(|mutation| {
                            matches!(mutation, CommitMutation::Append { path, .. } if path == own)
                        }).count() != 1
                    })
                    || prepared.reply.as_ref().is_some_and(|reply| {
                        request.mutations.iter().filter(|mutation| {
                            matches!(mutation, CommitMutation::Append { path, .. } if path == reply)
                        }).count() != 1 || request.mutations.iter().any(|mutation| {
                            matches!(mutation, CommitMutation::Fork { destination, .. } if covers(destination, reply))
                        })
                    })
                    || prepared.journal.as_ref().is_some_and(|journal| {
                        request.mutations.iter().filter(|mutation| mutation_targets(mutation, journal)).count()
                            != original.mutations.iter().filter(|mutation| mutation_targets(mutation, journal)).count()
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
                match &outcome {
                    Err(source @ (StreamError::Unavailable | StreamError::InvalidResponse)) => {
                        prepared.acknowledgement_unknown = true;
                        return Err(Error::Unknown {
                            publication: Box::new(prepared),
                            source: source.clone(),
                        });
                    }
                    Ok(_)
                    | Err(
                        StreamError::InvalidPath
                        | StreamError::InvalidArgument
                        | StreamError::LimitExceeded
                        | StreamError::NotFound
                        | StreamError::AlreadyExists
                        | StreamError::PrefixNotRetained
                        | StreamError::OutOfRange
                        | StreamError::HierarchyChanged
                        | StreamError::IdempotencyMismatch
                        | StreamError::Capacity
                        | StreamError::AccessDenied
                        | StreamError::DeadlineElapsed
                        | StreamError::Unsupported
                        | StreamError::RevisionForeign,
                    ) => {}
                }
                let retained = match &outcome {
                    Ok(CommitOutcome::Committed(receipt)) => prepared.retained(receipt),
                    Ok(CommitOutcome::Conflict(_)) | Err(_) => Ok(None),
                };
                if let Some(composition) = prepared.composition {
                    if let Err(source) = composition.settle(outcome.as_ref()).await {
                        return Err(Error::Settlement { outcome, source });
                    }
                }
                PreparedPublish::settle(&outcome?)?;
                retained
            }
        )
    }

    fn prepare(
        &self,
        previous: &Snapshot,
        state: State,
        effects: Vec<Effect>,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
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
                Some(self.activation(&self.actor(&previous.path)?)),
            ),
        };
        let encoded = state.encode();
        let mut prepared = PreparedPublish {
            own: Some(previous.path.clone()),
            state: Some(state),
            reply: None,
            journal: None,
            composition: None,
            sealed: false,
            acknowledgement_unknown: false,
            request: CommitRequest {
                deadline_unix_millis: None,
                conditions: vec![own],
                mutations: vec![CommitMutation::Append {
                    path: previous.path.clone(),
                    records: vec![encoded.into()],
                }],
                idempotency_key: key,
            },
        };
        for effect in created.into_iter().chain(effects) {
            prepared.join(effect)?;
        }
        Ok(prepared)
    }
}

fn covers(ancestor: &StreamPath, path: &StreamPath) -> bool {
    ancestor == path
        || path
            .as_str()
            .strip_prefix(ancestor.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn mutation_targets(mutation: &CommitMutation, path: &StreamPath) -> bool {
    match mutation {
        CommitMutation::Append {
            path: destination, ..
        } => destination == path,
        CommitMutation::Fork { destination, .. } => covers(destination, path),
    }
}

fn condition_path(condition: &CommitCondition) -> &StreamPath {
    match condition {
        CommitCondition::Head { path, .. }
        | CommitCondition::Tail { path, .. }
        | CommitCondition::Absent { path }
        | CommitCondition::Present { path } => path,
    }
}

fn owned(previous: &Snapshot, epoch: u64) -> Result<&State, Error> {
    previous
        .state
        .as_ref()
        .filter(|state| epoch != 0 && state.epoch == epoch)
        .ok_or(Error::Conflict)
}

/// The pure durable transition: consume exactly the trigger, then apply the
/// alarm change. Checkpoint, cursor and alarm move together or not at all.
fn next(
    mut state: State,
    original_alarm: &Alarm,
    trigger: &Trigger,
    transition: &Transition,
) -> Result<State, Error> {
    consume(&mut state, original_alarm, trigger)?;
    change_alarm(&mut state.alarm, transition.alarm)?;
    state.checkpoint = Some(transition.checkpoint.clone());
    Ok(state)
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
        }
        Trigger::Alarm {
            generation,
            now_unix_ms,
        } => {
            if original_alarm.generation != *generation
                || !original_alarm
                    .pending()
                    .is_some_and(|deadline| deadline <= *now_unix_ms)
            {
                return Err(Error::Conflict);
            }
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
