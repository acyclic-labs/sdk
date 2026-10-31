//! Existing authority appends contribute through their original group builder.
use super::*;
use crate::distributed::append::group::{GroupPreparation, group_first, merge_transaction};

#[cfg(test)]
mod tests;

/// Preparation of an ordered authority history in a caller's atomic commit.
#[must_use]
pub enum FilesystemAppendPreparation<'a> {
    /// No write has occurred; retain this confirmation beside the caller's commit.
    Ready(PreparedFilesystemAppend<'a>),
    /// Nothing was added to the caller's request or published.
    Rejected(AppendGroupOutcome),
}

/// Original authority receipts, borrowing the original immutable append inputs.
///
/// Keep this owner and every publication proof/hold alive in the same producer
/// as the contributed `FilesystemCommit`, including across an unknown result.
/// This is preparation, not accepted-effect custody or a new producer owner.
#[must_use]
pub struct PreparedFilesystemAppend<'a> {
    anchor: AuthoritySnapshot,
    receipts: crate::kernel::Owned<Vec<crate::distributed::append::AppendReceipt<'a>>>,
    owner: [u8; 16],
    scope: [u8; 16],
}

impl PreparedFilesystemAppend<'_> {
    /// Confirms the actual outcome of the same contributed commit. This uses
    /// the ordinary authority receipt/recovery path, with no new submission.
    /// This confirms the authority members only; the original caller validates
    /// its own receipt members before exposing its result. An error leaves
    /// this owner available for the original continuation.
    pub async fn confirm<P: crate::distributed::AuthorityProvider + ?Sized>(
        &self,
        store: &StreamAuthorityStore<P>,
        outcome: &CommitOutcome,
        work: &mut AuthorityWork<'_>,
    ) -> Result<AppendGroupOutcome, AuthorityStoreError> {
        if self.owner != store.operation_claims().owner_identity()
            || self.scope != store.actor_scope()
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "append confirmation belongs to another Streams owner",
            ));
        }
        store
            .settle_group(
                &self.receipts.value,
                self.anchor,
                outcome,
                work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| error.into_error(std::convert::identity))
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Adds an already composed serial history to the caller's original commit.
    ///
    /// Uses the same admission, provisional predecessors, policy fences,
    /// locators, namespace changes and projection as `compare_and_append_group`.
    /// It issues no provider write. Preparation failure leaves the original
    /// request unchanged; success preserves its key and tightens its deadline.
    /// The caller retains the original publication guards and before-publication
    /// contract, and confirms the actual receipt before exposing its result.
    pub async fn contribute_append_group<'a>(
        &self,
        commit: &mut FilesystemCommit,
        requests: &'a [crate::GuardedAppend],
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemAppendPreparation<'a>, AuthorityStoreError> {
        let owner = self.operation_claims().owner_identity();
        commit.check_owner(owner)?;
        let first = group_first(requests, cuts)?;
        commit.ensure_unclaimed(first.authority_id)?;
        let actor = self.actor(first.authority_id, work)?;
        let mut journal = actor.lease(work).await?;
        let projection = self
            .activate(
                first.authority_id,
                first.commit.publication.as_ref().map(|seal| seal.volume()),
                &mut journal,
                work,
            )
            .await?;
        let revision = actor.begin_publication(journal)?;
        let prepared = self
            .prepare_group(requests, work, None::<&mut ()>, Some(&projection))
            .await
            .map_err(|error| error.into_error(std::convert::identity))?;
        let prepared = match prepared {
            GroupPreparation::Ready(prepared) => prepared,
            GroupPreparation::Rejected {
                index,
                outcome,
                observed,
            } => {
                actor.complete(revision, projection, work).await?;
                return Ok(FilesystemAppendPreparation::Rejected(
                    AppendGroupOutcome::Rejected {
                        index,
                        outcome,
                        observed,
                    },
                ));
            }
        };
        let next_projection = prepared.project(&self.placement, projection, work)?;
        let mut part = self.with_policy_cuts(prepared.transaction, cuts, work)?;
        part.deadline_unix_millis = match (part.deadline_unix_millis, prepared.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        // Conditions may be strengthened and appends coalesced. Stage the
        // complete request so a later refusal cannot mutate the caller's cut.
        let mut request = work.copy_commit(commit.request())?;
        merge_transaction(&mut request, part, work)?;
        grow(&mut commit.authorities, 1, work)?;
        grow(&mut commit.contributions, 1, work)?;
        commit.request = request;
        commit.authorities.push(first.authority_id);
        commit.contributions.push(Contribution {
            actor,
            revision,
            projection: next_projection,
        });
        commit.bind_owner(owner);
        Ok(FilesystemAppendPreparation::Ready(
            PreparedFilesystemAppend {
                anchor: prepared.anchor,
                receipts: prepared.receipts,
                owner,
                scope: self.actor_scope(),
            },
        ))
    }
}
