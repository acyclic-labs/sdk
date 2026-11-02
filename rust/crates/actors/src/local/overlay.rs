//! Fork overlays: the actor layer gives a Streams fork its meaning with one
//! state record in the same commit. Streams `Fork` stays actor-blind; every
//! actor that exists was made by one of these two overlays over one fork.
//!
//! - [`Creation`] (spawn): a dormant actor. Its alarm is cleared, its reads
//!   are registered at the trigger's position but nothing is ready, and it
//!   carries its initial checkpoint and funding. No step runs until a trigger
//!   append (with [`Store::activation`]) arrives.
//! - [`Branch`] (counterfactual): a live copy of an actor's exact cut, born
//!   paused, with its own escrow (zero unless the forker allocates).
//!
//! A branch that is never resumed is a pinned cut: it never changes, so a
//! later fork from it (`Store::branch(&store.load(pin)?, ..)`) forks the
//! original subtree at that revision, retained by the pin's own path.
use super::{Alarm, Error, Snapshot, State, Store, Subscription};
use crate::addressing::ActorReference;
use crate::escrow::Escrow;
use crate::guest::{Checkpoint, Effect};
use acyclic_stream::{CommitCondition, CommitMutation, RelativePath, StreamPath, StreamProvider};
use std::collections::BTreeMap;

/// Subtree of an actor's state stream that a creation never inherits.
const JOURNAL: &str = "journal";

/// The creation overlay: a dormant actor that a trigger starts.
#[derive(Clone, Debug, Default)]
pub struct Creation {
    /// Instance-bound reads, keyed by manifest subscription id. Each cursor
    /// is the first record the actor consumes: the trigger's position.
    pub subscriptions: BTreeMap<String, Subscription>,
    /// The initial checkpoint the first step receives.
    pub checkpoint: Option<Checkpoint>,
    /// Funding allocated in this same commit ([`Escrow::allocate`] on the
    /// creator's balance, or the admitting host's root funding). `None`
    /// creates an unfunded actor, which cannot spend.
    pub escrow: Option<Escrow>,
}

/// The branch overlay: a paused, independently funded copy of an exact cut.
#[derive(Clone, Debug)]
pub struct Branch {
    /// The branch's own inputs, under the source's exact subscription ids.
    pub subscriptions: BTreeMap<String, Subscription>,
    /// Funding the forker allocated in this same commit; [`Branch::unfunded`]
    /// gives escrow zero. Funding inherited from the source is never usable.
    pub escrow: Escrow,
}

impl Branch {
    /// A branch with escrow zero over `subscriptions`.
    #[must_use]
    pub fn unfunded(subscriptions: BTreeMap<String, Subscription>) -> Self {
        Self {
            subscriptions,
            escrow: Escrow::new(Some(0)),
        }
    }
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// The effect that creates dormant `actor` inside a caller-built commit.
    /// With a `template`, the actor's state subtree is a copy-on-write fork
    /// of the template's exact cut (O(1) in its history), minus its journal;
    /// the overlay replaces every inherited field. Without one it starts
    /// empty. The caller adds its trigger and [`Store::activation`]. Create
    /// each subscribed stream in the same commit or earlier.
    pub fn create(
        &self,
        template: Option<&Snapshot>,
        actor: &ActorReference,
        creation: Creation,
    ) -> Result<Effect, Error> {
        let path = self.path(actor)?;
        if creation
            .subscriptions
            .values()
            .any(|read| read.path == path)
        {
            return Err(Error::Invalid("actor subscribes to its own state"));
        }
        let alarm = match template.and_then(Snapshot::state) {
            // Advance past the template's generation, so no delivery armed
            // for the template can fire in the new actor.
            Some(state) => Alarm {
                generation: state
                    .alarm
                    .generation
                    .checked_add(1)
                    .ok_or(Error::Invalid("alarm generation overflow"))?,
                deadline_unix_ms: None,
                delivered_generation: None,
            },
            None => Alarm::default(),
        };
        let state = State {
            epoch: 0,
            placement_branch: None,
            escrow: creation.escrow,
            checkpoint: creation.checkpoint,
            subscriptions: creation.subscriptions,
            alarm,
            paused: false,
            metadata: BTreeMap::new(),
            journal_tail: 0,
        };
        match template {
            None => Ok(Effect {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutation: CommitMutation::Append {
                    path,
                    records: vec![state.encode().into()],
                },
            }),
            Some(template) => fork(template, path, vec![StreamPath::new(JOURNAL)?], &state),
        }
    }

    /// The effect that forks `cut`'s exact state subtree into new actor
    /// `child` inside a caller-built commit: checkpoint, metadata, journal
    /// and alarm are shared, the branch is born paused, and its escrow is
    /// `branch.escrow`. It conflicts if the source moved past `cut` or
    /// `child` exists. The inherited placement cannot publish in the branch;
    /// resuming it claims a newer epoch against this fork (lazy fencing).
    pub fn branch(
        &self,
        cut: &Snapshot,
        child: &ActorReference,
        branch: Branch,
    ) -> Result<Effect, Error> {
        let state = cut
            .state
            .as_ref()
            .ok_or(Error::Invalid("actor state was never created"))?;
        let path = self.path(child)?;
        if !branch.subscriptions.keys().eq(state.subscriptions.keys()) {
            return Err(Error::Invalid("fork changes the actor's subscriptions"));
        }
        if branch.subscriptions.values().any(|read| read.path == path) {
            return Err(Error::Invalid("actor subscribes to its own state"));
        }
        let overlay = State {
            subscriptions: branch.subscriptions,
            escrow: Some(branch.escrow),
            paused: true,
            ..state.clone()
        };
        fork(cut, path, Vec::new(), &overlay)
    }
}

/// One Streams fork of `source`'s exact cut into `destination`, with the
/// overlay as the fork commit's own state record. The record's anchor is the
/// fork commit, which is how the first claim recognises funding the forker
/// wrote for this branch (see [`Snapshot::overlaid`]).
fn fork(
    source: &Snapshot,
    destination: StreamPath,
    exclude: Vec<RelativePath>,
    overlay: &State,
) -> Result<Effect, Error> {
    let commit_id = source
        .anchor
        .ok_or(Error::Invalid("actor state was never created"))?;
    if destination == source.path {
        return Err(Error::Invalid("actor cannot fork into itself"));
    }
    Ok(Effect {
        conditions: vec![
            CommitCondition::Head {
                path: source.path.clone(),
                expected: source.tail,
                commit_id,
            },
            CommitCondition::Absent {
                path: destination.clone(),
            },
        ],
        mutation: CommitMutation::Fork {
            exclude,
            source: source.path.clone(),
            destination,
            at_tail: source.tail,
            records: vec![overlay.encode().into()],
        },
    })
}
