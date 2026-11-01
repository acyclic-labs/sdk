//! Static forwarding to the sole Streams preparation and receipt path.
use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> crate::async_storage::AuthorityComposition
    for &StreamAuthorityStore<P>
{
    async fn contribute<'a>(
        &self,
        commit: &mut FilesystemCommit,
        requests: &'a [crate::GuardedAppend],
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemAppendPreparation<'a>, AuthorityStoreError> {
        self.contribute_append_group(commit, requests, cuts, work)
            .await
    }

    fn seal(
        &self,
        commit: &mut FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        StreamAuthorityStore::seal(self, commit, work)
    }

    async fn submit(
        &self,
        commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        StreamAuthorityStore::submit(self, commit, work).await
    }

    fn settle(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        self.settle_recorded(commit, outcome, |commit, commit_id| {
            self.refresh_committed(commit, commit_id);
        })
    }
}
