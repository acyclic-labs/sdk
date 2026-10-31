//! Optional atomic composition through the backend's original publication owner.

use super::*;
use crate::{FilesystemAppendPreparation, FilesystemCommit, FilesystemCommitOutcome, PolicyCut};

/// The original backend's prepared commit path. Obtaining this capability is
/// allocation-free; an unsupported backend returns `None` before staging work.
/// Every method retains the original request, construction work and settlement
/// owner. This capability does not authorize a caller or submit during preparation.
pub trait AuthorityComposition: ProviderPlatform {
    /// Add authenticated authority appends without submitting them.
    fn contribute<'a>(
        &self,
        commit: &mut FilesystemCommit,
        requests: &'a [GuardedAppend],
        cuts: &[PolicyCut],
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<FilesystemAppendPreparation<'a>, crate::AuthorityStoreError>>
    + ProviderTask;

    /// Freeze the original request and its policy fences before external submission.
    fn seal(
        &self,
        commit: &mut FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::AuthorityStoreError>;

    /// Submit once; an uncertain outcome returns the same sealed owner.
    fn submit(
        &self,
        commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> impl Future<Output = Result<FilesystemCommitOutcome, crate::AuthorityStoreError>> + ProviderTask;

    /// Confirm the complete original request before releasing publication holds.
    /// Missing refresh work skips only cache refresh, never receipt validation.
    fn settle(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::CommitOutcome,
        work: Option<&mut AuthorityWork<'_>>,
    ) -> impl Future<Output = Result<FilesystemCommitOutcome, crate::AuthorityStoreError>> + ProviderTask;
}

// The default capability is uninhabited: a backend must return None, never an
// executor which silently falls back to independently publishing a contribution.
impl AuthorityComposition for std::convert::Infallible {
    async fn contribute<'a>(
        &self,
        _commit: &mut FilesystemCommit,
        _requests: &'a [GuardedAppend],
        _cuts: &[PolicyCut],
        _work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemAppendPreparation<'a>, crate::AuthorityStoreError> {
        match *self {}
    }

    fn seal(
        &self,
        _commit: &mut FilesystemCommit,
        _work: &mut AuthorityWork<'_>,
    ) -> Result<(), crate::AuthorityStoreError> {
        match *self {}
    }

    async fn submit(
        &self,
        _commit: FilesystemCommit,
        _work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, crate::AuthorityStoreError> {
        match *self {}
    }

    async fn settle(
        &self,
        _commit: FilesystemCommit,
        _outcome: acyclic_stream::CommitOutcome,
        _work: Option<&mut AuthorityWork<'_>>,
    ) -> Result<FilesystemCommitOutcome, crate::AuthorityStoreError> {
        match *self {}
    }
}
