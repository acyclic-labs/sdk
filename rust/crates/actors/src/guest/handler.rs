//! Durable record handlers shared by native packages and guest execution adapters.
use super::{Checkpoint, CodecIdentity, GuestFuture};
use crate::{addressing::ActorReference, domain};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, Record, StreamError,
};

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
    /// Canonical explicit resource bindings.
    pub bindings: Vec<domain::Binding>,
    /// Canonical execution and checkpoint bounds.
    pub limits: domain::ActorLimits,
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

/// Invocation-scoped host input; it contains no deployment credentials.
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
pub struct Transition {
    /// Artifact-owned state encoded with the declared codec.
    pub checkpoint: Checkpoint,
    /// Alarm mutation, including clear-generation fencing.
    pub alarm: AlarmChange,
    /// Committed effects, never an independent best-effort dispatch path.
    pub effects: Vec<Effect>,
    /// Custody of one joined participant through sealing and the exact outcome.
    pub composition: Option<Box<dyn Composition>>,
}

/// One participant in the actor's sole publication, independent of its host.
/// The runtime retains it with the sealed request while acknowledgement is unknown.
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
    ) -> GuestFuture<'a, Transition>;
}
