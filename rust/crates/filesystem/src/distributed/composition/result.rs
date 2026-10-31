//! One receipt owner across validation, semantic confirmation and retirement.
use super::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// A provider result retaining its original backing capacity.
///
/// Borrow the value while processing it. The value is destroyed before the
/// original provider allocation retires; extracting an unfunded result is not
/// supported.
#[must_use]
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

/// An original sealed request and its completely validated committed receipt.
///
/// This retains construction capacity, publication holds and provider result
/// capacity while the caller confirms its semantic members. Only
/// [`StreamAuthorityStore::finish_settlement`] retires the filesystem
/// contributions and publishes their best-effort cache projections. Dropping
/// this owner leaves caches invalidated; it cannot undo the durable commit.
#[must_use]
pub struct PreparedFilesystemCommit {
    commit: FilesystemCommit,
    receipt: FilesystemCommitResult<acyclic_stream::CommitReceipt>,
}

impl PreparedFilesystemCommit {
    /// The exact original normalized request, including key and deadline.
    pub fn request(&self) -> &CommitRequest {
        self.commit.request()
    }

    /// The original complete receipt, without copying or releasing its funding.
    pub fn receipt(&self) -> &acyclic_stream::CommitReceipt {
        &self.receipt
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    /// Validates a caller-submitted portable result before semantic confirmation.
    ///
    /// Every canonical append/fork and range is checked once. A conditional
    /// loser returns `Rejected`; any refusal returns the unchanged `Unknown`
    /// request. Neither path advances a cache or marks a publication hold.
    pub fn prepare_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
    ) -> Result<PreparedFilesystemCommit, FilesystemCommitOutcome> {
        self.prepare_retained_settlement(commit, outcome, None)
    }

    /// Transfers the original native provider result into semantic confirmation.
    /// Its original allocation remains with both committed and rejected values.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn prepare_native_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::NativeStreamOutput<CommitOutcome>,
    ) -> Result<PreparedFilesystemCommit, FilesystemCommitOutcome> {
        let (outcome, allocation) = outcome.into_parts();
        self.prepare_retained_settlement(commit, outcome, Some(allocation))
    }

    pub(super) fn prepare_retained_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
        allocation: super::super::io::ProviderAllocation,
    ) -> Result<PreparedFilesystemCommit, FilesystemCommitOutcome> {
        if let Err(error) = commit
            .check_owner(self.operation_claims().owner_identity())
            .and_then(|()| commit.ensure_sealed())
        {
            // Destroy the original value before its allocation on refusal too.
            drop(outcome);
            drop(allocation);
            return Err(FilesystemCommitOutcome::Unknown { commit, error });
        }
        match outcome {
            CommitOutcome::Committed(envelope) => {
                let receipt = FilesystemCommitResult {
                    value: envelope,
                    _allocation: allocation,
                };
                if let Err(error) = receipt.validate_request(commit.request()) {
                    return Err(FilesystemCommitOutcome::Unknown {
                        commit,
                        error: map_stream_error(error),
                    });
                }
                Ok(PreparedFilesystemCommit { commit, receipt })
            }
            CommitOutcome::Conflict(conflicts) => {
                Err(FilesystemCommitOutcome::Rejected(FilesystemCommitResult {
                    value: conflicts,
                    _allocation: allocation,
                }))
            }
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
        let PreparedFilesystemCommit { commit, receipt } = prepared;
        if let Err(error) = commit.check_owner(self.operation_claims().owner_identity()) {
            return Ok(FilesystemCommitOutcome::Unknown { commit, error });
        }
        refresh(commit.recorded(), receipt.commit_id);
        Ok(FilesystemCommitOutcome::Committed(receipt))
    }
}
