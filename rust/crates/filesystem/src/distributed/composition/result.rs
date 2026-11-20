//! One receipt owner across validation, semantic confirmation and retirement.
use super::*;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;

/// A provider result retaining its original backing capacity.
///
/// Borrow the value while processing it. The value is destroyed before the
/// original provider allocation retires; extracting an unfunded result is not
/// supported.
pub struct FilesystemCommitResult<T> {
    value: T,
    _allocation: super::super::io::ProviderAllocation,
}

impl<T> std::ops::Deref for FilesystemCommitResult<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for FilesystemCommitResult<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(formatter)
    }
}

/// An original sealed request and its retained provider decision.
///
/// This retains construction capacity, publication holds and provider result
/// capacity while the caller confirms its semantic members. Only
/// [`StreamAuthorityStore::finish_settlement`] retires the filesystem
/// contributions and publishes their best-effort cache projections. Dropping
/// this owner leaves caches invalidated; it cannot undo the durable commit.
#[must_use]
pub struct PreparedFilesystemCommit {
    commit: FilesystemCommit,
    outcome: FilesystemCommitResult<CommitOutcome>,
}

/// A settlement's preparation: the validated original result, or the
/// unchanged commit and why its result was refused.
///
/// A refusal returns custody without allocating another owner for it.
#[must_use]
pub enum FilesystemSettlement {
    /// The result passed validation and awaits semantic confirmation.
    Prepared(PreparedFilesystemCommit),
    /// Nothing advanced. Retry exactly this commit: its idempotency key and
    /// bytes are unchanged.
    Refused {
        /// The unchanged commit to resubmit.
        commit: FilesystemCommit,
        /// Why its result was refused.
        error: AuthorityStoreError,
    },
}

impl PreparedFilesystemCommit {
    /// The exact original normalized request, including key and deadline.
    pub fn request(&self) -> &CommitRequest {
        self.commit.request()
    }

    /// The original decision, without copying or releasing its funding.
    /// Committed receipts have passed complete canonical request validation.
    pub fn outcome(&self) -> &CommitOutcome {
        &self.outcome
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Validates a caller-submitted portable result before semantic confirmation.
    ///
    /// Every canonical append/fork and range is checked once. A conditional
    /// loser remains available for semantic confirmation; any refusal returns
    /// the unchanged `Unknown` request. Neither path advances a cache or marks
    /// a publication hold.
    pub fn prepare_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
    ) -> FilesystemSettlement {
        self.prepare_retained_settlement(commit, outcome, None)
    }

    /// Transfers the original native provider result into semantic confirmation.
    /// Its original allocation remains with both committed and rejected values.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_native_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::NativeStreamOutput<CommitOutcome>,
    ) -> FilesystemSettlement {
        let (outcome, allocation) = outcome.into_parts();
        self.prepare_retained_settlement(commit, outcome, Some(allocation))
    }

    pub(super) fn prepare_retained_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
        allocation: super::super::io::ProviderAllocation,
    ) -> FilesystemSettlement {
        // The result destroys its value before its allocation, on refusal too.
        let outcome = FilesystemCommitResult {
            value: outcome,
            _allocation: allocation,
        };
        let refusal = commit
            .check_owner(self.operation_claims().owner_identity())
            .and_then(|()| commit.ensure_sealed())
            .and_then(|()| match &*outcome {
                CommitOutcome::Committed(receipt) => receipt
                    .validate_request(commit.request())
                    .map_err(map_stream_error),
                CommitOutcome::Conflict(_) => Ok(()),
            });
        match refusal {
            Ok(()) => FilesystemSettlement::Prepared(PreparedFilesystemCommit { commit, outcome }),
            Err(error) => FilesystemSettlement::Refused { commit, error },
        }
    }

    /// Completes the filesystem side after the caller confirms its own members.
    /// Complete request validation is not repeated. No provider write occurs.
    pub fn finish_settlement(
        &self,
        prepared: PreparedFilesystemCommit,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        self.finish_recorded(prepared, |commit, commit_id| {
            self.refresh_committed(commit, commit_id);
        })
    }

    pub(super) fn finish_recorded(
        &self,
        prepared: PreparedFilesystemCommit,
        refresh: impl FnOnce(RecordedCommit, acyclic_stream::CommitId),
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        let PreparedFilesystemCommit { commit, outcome } = prepared;
        if let Err(error) = commit.check_owner(self.operation_claims().owner_identity()) {
            return Ok(FilesystemCommitOutcome::Unknown { commit, error });
        }
        let FilesystemCommitResult { value, _allocation } = outcome;
        Ok(match value {
            CommitOutcome::Committed(receipt) => {
                refresh(commit.recorded(), receipt.commit_id);
                FilesystemCommitOutcome::Committed(FilesystemCommitResult {
                    value: receipt,
                    _allocation,
                })
            }
            CommitOutcome::Conflict(conflicts) => {
                FilesystemCommitOutcome::Rejected(FilesystemCommitResult {
                    value: conflicts,
                    _allocation,
                })
            }
        })
    }
}
