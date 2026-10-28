//! Observation of an explicit input cut through the original actor state stream.
use super::{Error, Snapshot, State, Store};
use crate::addressing::ActorReference;
use acyclic_stream::{CommitId, FollowRequest, Record, RevisionCut, StreamPath, StreamProvider};
use futures::TryStreamExt;

/// A requested consumed prefix of one instance-bound subscription.
/// This is observation metadata, never execution or storage authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedCut {
    subscription: String,
    path: StreamPath,
    next: u64,
    preceding: Option<CommitId>,
    at_least: RevisionCut,
}

impl AppliedCut {
    /// The empty prefix still names its exact subscription and path.
    pub fn empty(subscription: String, path: StreamPath) -> Result<Self, Error> {
        if subscription.is_empty() {
            return Err(Error::Invalid("applied cut has no subscription"));
        }
        Ok(Self {
            subscription,
            path,
            next: 0,
            preceding: None,
            at_least: RevisionCut::default(),
        })
    }

    /// Include this original committed input record in the requested prefix.
    /// The provider verifies its sequence/anchor before observing actor state.
    pub fn through(subscription: String, path: StreamPath, record: &Record) -> Result<Self, Error> {
        let mut cut = Self::empty(subscription, path)?;
        cut.next = record
            .sequence
            .checked_add(1)
            .ok_or(Error::Invalid("applied cut cursor overflow"))?;
        cut.preceding = Some(record.commit_id);
        Ok(cut)
    }

    /// Manifest subscription identity.
    pub fn subscription(&self) -> &str {
        &self.subscription
    }
    /// Original instance-bound input path.
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }
    /// Requested exclusive consumed input cursor.
    pub const fn next(&self) -> u64 {
        self.next
    }
    /// Original input publication immediately before `next`, absent at zero.
    pub const fn preceding(&self) -> Option<CommitId> {
        self.preceding
    }

    /// Require the provider to apply these authority revisions before observing
    /// the input. This storage floor does not prove actor consumption.
    #[must_use]
    pub fn at_least(mut self, revision: RevisionCut) -> Self {
        self.at_least = revision;
        self
    }

    fn reached(&self, state: &State) -> Result<bool, Error> {
        let subscription = state
            .subscriptions()
            .get(&self.subscription)
            .filter(|subscription| subscription.path == self.path)
            .ok_or(Error::Conflict)?;
        Ok(subscription.cursor >= self.next)
    }
}

/// An original committed state observation that includes the requested cut.
/// It says nothing about a later pause, replacement or external execution.
pub struct Applied {
    snapshot: Snapshot,
    cut: AppliedCut,
    commit_id: CommitId,
}

impl Applied {
    /// The exact state whose committed subscription cursor satisfies the cut.
    pub const fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    /// The original requested input prefix, including its input anchor.
    pub const fn cut(&self) -> &AppliedCut {
        &self.cut
    }
    /// Original actor state publication, independently of its input anchor.
    pub const fn commit_id(&self) -> CommitId {
        self.commit_id
    }
    /// Exclusive actor state revision, including placement/control publications.
    pub const fn revision(&self) -> u64 {
        self.snapshot.tail
    }
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Wait for a committed state that consumed an explicit subscription cut.
    /// No handler, alarm, cursor or effect is advanced by this observation.
    /// Cancellation drops both provider follows; there is no polling task.
    pub async fn wait_applied(
        &self,
        actor: &ActorReference,
        cut: AppliedCut,
    ) -> Result<Applied, Error> {
        // Check the input history even if the actor already passed the cursor.
        // Never let a same-sequence record from another history satisfy it.
        if cut.preceding.is_some() || !cut.at_least.is_empty() {
            drop(
                self.provider
                    .follow(FollowRequest {
                        preceding: cut.preceding,
                        at_least: cut.at_least.clone(),
                        ..FollowRequest::new(cut.path.clone(), cut.next)
                    })
                    .await?,
            );
        }
        let mut snapshot = self.load(actor).await?;
        let state = snapshot
            .state()
            .ok_or(Error::Invalid("actor state was never created"))?;
        let anchor = snapshot
            .anchor
            .ok_or(Error::Invalid("actor state has no committed anchor"))?;
        if cut.reached(state)? {
            return Ok(Applied {
                snapshot,
                cut,
                commit_id: anchor,
            });
        }
        // Atomically subscribe at the exact loaded cut. Publications between
        // load and registration are replayed; history replacement is refused.
        let records = self
            .provider
            .follow(FollowRequest {
                preceding: Some(anchor),
                at_least: cut.at_least.clone(),
                ..FollowRequest::new(snapshot.path.clone(), snapshot.tail)
            })
            .await?;
        let mut records = acyclic_stream::records_only(records);
        while let Some(record) = records.try_next().await? {
            if record.sequence != snapshot.tail {
                return Err(Error::Invalid("applied observation skipped actor state"));
            }
            let state = State::decode(&record.value)?;
            let reached = cut.reached(&state)?;
            snapshot.tail = snapshot
                .tail
                .checked_add(1)
                .ok_or(Error::Invalid("actor state revision overflow"))?;
            snapshot.anchor = Some(record.commit_id);
            snapshot.state = Some(state);
            if reached {
                return Ok(Applied {
                    snapshot,
                    cut,
                    commit_id: record.commit_id,
                });
            }
        }
        Err(acyclic_stream::StreamError::Unavailable.into())
    }
}
