//! Durable record handlers shared by native packages and guest execution adapters.
use super::{Checkpoint, CodecIdentity, GuestError, GuestFuture};
use crate::{addressing::ActorReference, domain};
use acyclic_stream::{
    CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitRequest, Record, StreamError,
    StreamPath,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Immutable deployment metadata; resource selection is explicit, never inherited.
pub struct Manifest {
    /// Canonical artifact digest admitted by the deployment service.
    pub code: domain::CodeSha256,
    /// Codec written by this artifact.
    pub codec: CodecIdentity,
    /// Declared subscription ids. Each actor binds its own stream path and
    /// first cursor when its state is created, so one package serves many
    /// actors whose inputs live at different paths.
    pub subscriptions: Vec<String>,
    /// Records that cooperatively interrupt an in-flight delivery. These
    /// filters observe later records without advancing committed cursors.
    pub interrupts: Vec<Interrupt>,
    /// Canonical explicit resource bindings.
    pub bindings: Vec<domain::Binding>,
    /// Canonical execution and checkpoint bounds.
    pub limits: domain::ActorLimits,
}

/// One byte-prefix filter on a declared subscription's canonical values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interrupt {
    /// Declared subscription to observe.
    pub subscription: String,
    /// Nonempty canonical record prefix.
    pub value_prefix: Bytes,
}

impl Manifest {
    pub(crate) fn validate_interrupts(&self) -> Result<(), GuestError> {
        if self.interrupts.iter().any(|filter| {
            filter.value_prefix.is_empty() || !self.subscriptions.contains(&filter.subscription)
        }) {
            return Err(GuestError::Decode("invalid interrupt filter".into()));
        }
        Ok(())
    }
}

/// Durable identity of a delivery, independent of physical handler attempts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Trigger {
    /// Consume exactly the next record of one declared subscription.
    Record {
        /// Manifest subscription identity.
        subscription: String,
        /// Zero-based source record sequence.
        sequence: u64,
    },
    /// Consume only the currently retained alarm generation.
    Alarm {
        /// Generation captured when this wake was queued.
        generation: u64,
        /// Clock observation admitted by the host, in Unix milliseconds.
        now_unix_ms: u64,
    },
}

/// One instance-bound input: the actor's own stream and its next sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    /// Source stream, bound when the actor's state is created.
    pub path: StreamPath,
    /// Next unconsumed sequence.
    pub cursor: u64,
}

/// Read-only subscription projection bound to its original committed state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionCuts {
    anchor: CommitId,
    entries: BTreeMap<String, Subscription>,
}

impl SubscriptionCuts {
    /// Construct a host's projection of one admitted, size-bounded state.
    /// This metadata conveys no provider authority or freshness guarantee.
    pub fn new(anchor: CommitId, entries: BTreeMap<String, Subscription>) -> Self {
        Self { anchor, entries }
    }

    /// Original state publication; the checkpoint, epoch and cuts share it.
    pub const fn anchor(&self) -> CommitId {
        self.anchor
    }

    /// One declared subscription, including an unconsumed zero cursor.
    pub fn get(&self, id: &str) -> Option<&Subscription> {
        self.entries.get(id)
    }

    /// All admitted subscriptions in canonical id order, borrowed for this view.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&String, &Subscription)> {
        self.entries.iter()
    }
}

/// Invocation-scoped host input; it contains no deployment credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// Complete account/namespace/id address.
    pub actor: ActorReference,
    /// Placement fence admitted by the host.
    pub epoch: u64,
    /// Stable logical delivery identity.
    pub trigger: Trigger,
    /// Host clock observation for this attempt, in Unix milliseconds.
    pub now_unix_ms: u64,
    /// The consumed record for `Trigger::Record`; `None` for alarms.
    pub record: Option<Record>,
    /// Committed cuts from the same admitted state as the checkpoint and epoch.
    /// Present for record and alarm deliveries, including zero cursors. This is
    /// a read-only projection of the already loaded, size-bounded actor state.
    /// It cannot change subscriptions or advance their cursors.
    pub subscriptions: SubscriptionCuts,
}

/// Change to the actor's sole durable alarm, committed with its checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlarmChange {
    /// Preserve the alarm generation and deadline. Delivery consumption remains
    /// part of the same checkpoint, so a delivered generation cannot fire twice.
    Keep,
    /// Replace the alarm with a strictly positive absolute deadline.
    Set {
        /// Unix milliseconds.
        deadline_unix_ms: u64,
    },
    /// Clear the alarm and invalidate any previously queued wake.
    Clear,
}

/// One mutation published in the same transaction as the consumed input.
/// Conditions are explicit, so a send can be `Absent{<to>/closed}` plus an
/// append to `<to>`, and a spawn can carry a `Fork`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effect {
    /// Exact admitted cuts, retained unchanged for request replay.
    pub conditions: Vec<CommitCondition>,
    /// Append or Fork; nothing executes before the checkpoint commit.
    pub mutation: CommitMutation,
}

/// One indivisible logical result. Physical execution may repeat after a crash.
/// It is plain data, so it crosses a guest boundary through [`super::wire`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transition {
    /// Artifact-owned state encoded with the declared codec.
    pub checkpoint: Checkpoint,
    /// Alarm mutation, including clear-generation fencing.
    pub alarm: AlarmChange,
    /// Committed effects, never an independent best-effort dispatch path.
    pub effects: Vec<Effect>,
    /// One participant to join to the publication, as a request the host's
    /// [`Composer`] validates and composes; never guest-held custody.
    pub participant: Option<Participant>,
}

/// A request to join one host participant (for example spawned volumes) to
/// the publication. It comes from a possibly untrusted guest: the host's
/// [`Composer`] re-derives and validates everything it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Participant {
    /// Participant kind, selecting the host composer's interpretation.
    pub kind: String,
    /// Kind-defined canonical request bytes.
    pub plan: Bytes,
}

/// Host-side interpreter of [`Participant`] requests. The store opens each
/// request once, then owns the participant through seal, exact retry and
/// terminal settlement.
pub trait Composer: acyclic_stream::ProviderPlatform + 'static {
    /// Validate `participant` for `actor`'s transition from the committed
    /// `previous` checkpoint to `next`, and open its custody. `delivered` is
    /// the committed record the host delivered (never the handler's copy):
    /// before the first checkpoint it is the only committed account of the
    /// actor's creation.
    fn open(
        &self,
        actor: &ActorReference,
        previous: Option<&Checkpoint>,
        delivered: Option<&Record>,
        next: &Checkpoint,
        participant: &Participant,
    ) -> Result<Box<dyn Composition>, GuestError>;
}

/// One host participant in the actor's sole publication, opened by a
/// [`Composer`]. The store retains it with the sealed request while
/// acknowledgement is unknown.
pub trait Composition: acyclic_stream::ProviderPlatform + 'static {
    /// Extend and fence the joined request once, before its first submission.
    /// The actor's original conditions and mutations must remain intact.
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest>;

    /// Release or adopt custody from the exact terminal provider outcome or
    /// definite refusal. Uncertain acknowledgements never reach settlement.
    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()>;
}

/// The sole durable record-handler contract, independent of host and OS.
pub trait Handler: acyclic_stream::ProviderPlatform {
    /// The package's immutable code, codec, subscriptions and bindings.
    fn manifest(&self) -> &Manifest;
    /// Compute a transition without independently acknowledging input or effects.
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: super::CancellationToken,
    ) -> GuestFuture<'a, Transition>;
}
